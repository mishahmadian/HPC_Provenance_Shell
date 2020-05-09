#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# |------------------------------------------------------|
# |          HPC Provenance Client CLI Interface         |
# |                     Version 1.0                      |
# |                                                      |
# |       High Performance Computing Center (HPCC)       |
# |               Texas Tech University                  |
# |                                                      |
# |       Misha Ahmadian (misha.ahmadian@ttu.edu)        |
# |------------------------------------------------------|
#
import urllib.request, urllib.error
from collections import OrderedDict
from subprocess import Popen, PIPE
from tabulate import tabulate
from datetime import datetime
from itertools import chain
from enum import Enum
from cmd import Cmd
import hashlib
import json
import sys

#========== Global Variables ===========
API_SERVER_ADDR = "129.118.104.152"
API_SERVER_PORT = "5000"
CLUSTER_NAME = "genius"
SCHEDULER = "uge"
#=======================================

class RESTful_API:
    """
        This class provides required methods to communicaate with Provenance RestAPI
    """
    def __init__(self):
        global API_SERVER_ADDR, API_SERVER_PORT
        host = API_SERVER_ADDR
        port = API_SERVER_PORT
        self._api_url = "http://" + host + ":" + port

    def get(self, url_cmd: str, **kwargs):
        """
            Send a 'GET' request to Provenance Restful Server
        """
        try:
            # Generate the proper request URL
            req_url = self._gen_api_url(url_cmd, **kwargs)
            # Send request and get response
            response = urllib.request.urlopen(req_url)
            if response:
                # Decode the response and jsonify
                resp = json.loads(response.read().decode(response.info().get_param('charset') or 'utf-8'))
            else:
                resp = {}
            return resp

        except urllib.error.HTTPError as httpexp:
            ignore_codes = [404]
            if httpexp.code in ignore_codes:
                pass

        except urllib.error.URLError as urlexp:
            print("\n [Error] Provenance Cli cannot connect to the API server\n")
            sys.exit(1)

        except Exception as exp:
            print(str(exp))
            sys.exit(1)

    def get_oss_jobs_table(self, server, target=None, **kwargs):
        """
            Request the API for cluster Jobs and their OSS activities
        """
        url_cmd = f"/oss/{server}" + (f"/{target}" if target else "")
        # Call the Rest API
        api_response = self.get(url_cmd, **kwargs)
        # Get Query results
        results = api_response.get("result", None) if api_response else {}

        # tabulate results
        if results:
            headers = ["JobID", "TaskID", "User", "Stauts", "Read OPs", "Total Read Size",
                       "Write OPs", "Total Write Size"]
            if not target: headers.append("OST")
            table = []
            for data in results:
                row = [data["job_info"][0]["jobid"], data["job_info"][0]["taskid"], data["job_info"][0]["username"],
                    data["job_info"][0]["status"], data["read_bytes"], self._convert_from_byte(data["read_bytes_sum"]),
                    data["write_bytes"], self._convert_from_byte(data["write_bytes_sum"])]

                if not target: row.append(data["ost_target"])
                table.append(row)
            # Make a Table
            return tabulate(table, headers=headers, tablefmt="simple", stralign="right", numalign="right")

        # Otherwise Print no output data
        return "** No Result Found **"

    def get_mds_jobs_table(self, server, target=None, **kwargs):
        """
            Request the API for cluster Jobs and their MDS activities
        """
        url_cmd = f"/mds/{server}" + (f"/{target}" if target else "")
        # Call the Rest API
        api_response = self.get(url_cmd, **kwargs)
        # Get Query results
        results = api_response.get("result", None) if api_response else {}

        # tabulate results
        if results:
            headers = ["JobID", "TaskID", "User", "Stauts", "Open/Close", "Get/Set Attr", "mkdir/rmdir", "rm/unlink"]
            if not target: headers.append("MDT")
            table = []
            for data in results:
                row = [data["job_info"][0]["jobid"], data["job_info"][0]["taskid"], data["job_info"][0]["username"],
                       data["job_info"][0]["status"], f"{data['open']}/{data['close']}",
                       f"{data['getattr']}/{data['setattr']}", f"{data['mkdir']}/{data['rmdir']}", f"{data['unlink']}"]

                if not target: row.append(data['mdt_target'])
                table.append(row)
            # Make a Table
            return tabulate(table, headers=headers, tablefmt="simple", stralign="right", numalign="right")

        # Otherwise Print no output data
        return "** No Result Found **"

    def get_mds_files_Table(self, server, target, **kwargs):
        """
            Request the API for current file operation on a Target MDT
        """
        url_cmd = f"/mds/{server}" + (f"/{target}" if target else "")
        # Call the Rest API
        api_response = self.get(url_cmd, **kwargs)
        # Get Query results
        results = api_response.get("result", None) if api_response else {}

        if results:
            headers = ["JobID", "TaskID", "User", "Stauts", "File/Dir OP", "File/Dir Path"]
            table = []
            for data in results:
                # Replace the type UNLNK with REMOVE which is more meaningful
                if data["op_type"] and data["op_type"] == "UNLNK":
                    data["op_type"] = "REMOVE"

                row = [data["job_info"][0]["jobid"], data["job_info"][0]["taskid"], data["job_info"][0]["username"],
                       data["job_info"][0]["status"], data["op_type"]]

                # Find the absolute path of the file or directory
                file_dir_path = self._find_absolute_path(data["target_file"], data["target_path"], data["parent_path"])

                row.append(file_dir_path)
                table.append(row)

            # Make a Table
            return tabulate(table, headers=headers, tablefmt="simple", stralign="left", numalign="right")

        # Otherwise Print no output data
        return "** No Result Found **"


    def get_all_jobs(self, **kwargs):
        """
            Request API for all the recorded jobs information
        """
        url_cmd = "/jobinfo"
        # Call the Rest API
        api_response = self.get(url_cmd, **kwargs)
        # Get Query results
        results = api_response.get("result", None) if api_response else {}

        if results:
            headers = ["JobID", "TaskID", "JobName", "User", "Stauts", "Project", "PE", "# CPU",
                       "Submitted", "Started", "Finished"]
            table = []
            for data in results:
                row = [data["jobid"], data["taskid"], data["jobName"], data["username"], data["status"],
                       data["project"], data["parallelEnv"], data["num_cpu"]]

                # Convert epoch timestamps to readable date/time format
                for timestamp in [data["submit_time"], data["start_time"], data["end_time"]]:
                    if timestamp:
                        row.append(datetime.fromtimestamp(float(timestamp) / 1000.0).strftime("%m.%d.%Y\n%H:%M:%S"))
                    else:
                        row.append("N/A")
                table.append(row)

            # Make a Table
            return tabulate(table, headers=headers, tablefmt="simple", stralign="center", numalign="center")

        # Otherwise Print no output data
        return "** No Result Found **"

    def get_job_detail(self, jobid, taskid=None):
        """
            Request API and get all the details regarding a job
        """
        # Calculate the uid for the job
        uid = self._uniqID(jobid, taskid)
        url_cmd = f"/jobinfo/{uid}"
        # Call the Rest API
        api_response = self.get(url_cmd)
        # Get Query results
        results = api_response.get("result", None) if api_response else {}

        if results and results[0]:
            data = results[0]
            data_table = "[JOB INFO]\n\n"
            headers = ["JobID", "TaskID", "JobName", "User", "Stauts", "Project", "PE", "# CPU",
                       "Submitted", "Started", "Finished"]

            #--------------- JOB INFO 1st part ------------------
            row = [data["jobid"], data["taskid"], data["jobName"], data["username"], data["status"],
                   data["project"], data["parallelEnv"], data["num_cpu"]]

            # Convert epoch timestamps to readable date/time format
            for timestamp in [data["submit_time"], data["start_time"], data["end_time"]]:
                if timestamp:
                    row.append(datetime.fromtimestamp(float(timestamp) / 1000.0).strftime("%m.%d.%Y\n%H:%M:%S"))
                else:
                    row.append("N/A")

            data_table += tabulate([row], headers=headers, tablefmt="simple", stralign="center", numalign="center")

            # --------------- JOB INFO 2nd part ------------------
            headers = ["Parameters", "Values", "Parametes", "Values"]
            col1 = [["Queue", data["queue"]], ["h_vmem", data["h_vmem"]], ["Hard Runtime", data["h_rt"]],
                    ["Soft Runtime", data["s_rt"]]]

            col2 = [["Working Directory", data["pwd"]], ["Command", data["command"]],
                    ["Deleted Job", ', '.join(filter(None, data["q_del"]))],
                    ["Failed", ("yes" if data["failed_no"] else "No")]]

            two_col_tbl = [c1+c2 for c1,c2 in zip(col1, col2)]
            data_table += "\n" + tabulate(two_col_tbl, headers=headers, tablefmt="psql",
                                          stralign="left", numalign="left") + "\n\n"

            # --------------- MDS Dynamic Table ------------------
            mds_list = data["mds_data"]
            mds_map = {}
            mds_table = []

            # Map all the MDS Data into their MDS Server
            for mdsData in mds_list:
                mds = mdsData.pop("mds_host")
                if mds in mds_map.keys():
                    mds_map[mds].append(mdsData)
                else:
                    mds_map[mds] = [mdsData]

            # Create MDS Tables
            for mds, mdsDataLst in mds_map.items():
                data_table += f"[MDS: {mds}]\n"
                headers = [title for inx in range(len(mdsDataLst))
                            for title in [f"({mdsDataLst[inx].pop('mdt_target')})", "Stats"]]

                mdt_table = []
                for mdsData in mdsDataLst:
                    col = [[key, str(value)] for key, value in mdsData.items() if key != "file_op"]
                    mdt_table.append(col)

                # Create OST Table
                mds_table = list(map(lambda x: list(chain(*x)), zip(*mdt_table)))

                data_table += "\n" + tabulate(mds_table, headers=headers, tablefmt="psql",
                                              stralign="left", numalign="left") + "\n\n"

            # --------------- OSS Dynamic Table ------------------
            oss_list = data["oss_data"]
            oss_map = {}

            # Map all the OSS Data into their OSS Server name
            for ossData in oss_list:
                oss = ossData.pop("oss_host")
                if oss in oss_map.keys():
                    oss_map[oss].append(ossData)
                else:
                    oss_map[oss] = [ossData]

            # Create OSS Tables
            for oss, ossDataLst in oss_map.items():
                data_table += f"[MDS: {oss}]\n"

                headers = [title for inx in range(len(ossDataLst))
                            for title in [f"({ossDataLst[inx].pop('ost_target')})", "Stats"]]
                ost_table = []
                for ossData in ossDataLst:
                    col = [[key, self._convert_from_byte(value) if "_bytes_" in key else str(value)]
                                for key, value in ossData.items()]
                    ost_table.append(col)

                # Create OST Table
                oss_table = list(map(lambda x: list(chain(*x)), zip(*ost_table)))

                data_table += "\n" + tabulate(oss_table, headers=headers, tablefmt="psql",
                                              stralign="left", numalign="left") + "\n\n"

            # --------------- Files and File OPs per Jobs ------------------
            # Map The File OPs to their MDT target
            fileop_map = {}
            for mdsData in data["mds_data"]:
                for fileObj in mdsData["file_op"]:
                    if fileObj["mdtTarget"] in fileop_map:
                        fileop_map[fileObj.pop("mdtTarget")].append(fileObj)
                    else:
                        fileop_map[fileObj.pop("mdtTarget")] = [fileObj]

            # Create File OP Table
            for mdt_target, fileop_lst in fileop_map.items():
                data_table += f"[Files on ({mdt_target})]\n"
                headers = ["Date/Time", "From", "OP", "Mode", "File/Dir Path"]
                table = []
                for fileobj in fileop_lst:
                    row = [
                        datetime.fromtimestamp(int(fileobj["timestamp"])).strftime("%m-%d-%Y\n%H:%M:%S"),
                        fileobj["nid"] if fileobj["nid"] else "N/A",
                        fileobj["op_type"] if fileobj["op_type"] != "UNLNK" else "REMOVE",
                        fileobj["open_mode"],
                        self._find_absolute_path(fileobj["target_file"],
                                                 fileobj["target_path"],
                                                 fileobj["parent_path"])
                    ]
                    table.append(row)

                #Make table:
                data_table += "\n" + tabulate(table, headers=headers, tablefmt="simple",
                                              stralign="left", numalign="left") + "\n\n"


            # Print
            return data_table


        # Otherwise Print no output data
        return "** No Result Found **"



    def _gen_api_url(self, url_cmd: str, **kwargs):
        """
            Generate the proper API URL to be sent as a request
        """
        if not url_cmd.startswith('/'):
            url_cmd = '/' + url_cmd
        # Generate the Request URL
        req_url = self._api_url + '/provenance' + url_cmd
        # If keyword arguments appeared then:
        if kwargs:
            # Extract the jobid and taskid
            jobid = kwargs.pop('jobid') if kwargs.get('jobid', None) else None
            taskid = kwargs.pop('taskid') if kwargs.get('taskid', None) else None
            # Generate UID and update args
            if jobid:
                uid = self._uniqID(jobid, taskid)
                kwargs.update({'uid': uid})

            # Construct the URL Key/Value list if applicable
            req_url += "?"
            # Append any key/value if specified
            for key, value in kwargs.items():
                req_url += f"{key}={value}&"
            # Remove the last '&'
            req_url = req_url[:-1]
        # Return the URL
        return req_url

    @staticmethod
    def _uniqID(jobid, taskid=None):
        """
        Generate the 'uid' out of jobid and taskid
        """
        global CLUSTER_NAME, SCHEDULER
        # calculate the MD5 hash
        obj_id = ''.join(filter(None, [SCHEDULER, CLUSTER_NAME, jobid, taskid]))
        hash_id = hashlib.md5(obj_id.encode(encoding='UTF=8'))
        return hash_id.hexdigest()

    @staticmethod
    def _convert_from_byte(data_byte_str):
        """
            Convert byt to TB, GB, MB, or KB
        """
        units = ["KB", "MB", "GB", "TB"]
        data_byte = float(data_byte_str)
        data_unit = "B"
        # Convert byte to one unit up until it matches
        while units:
            if data_byte < 1024:
                break
            data_byte /= 1014
            data_unit = units.pop(0)
        return "%.2f %s" % (data_byte, data_unit)

    @staticmethod
    def _find_absolute_path(target_file, target_path, parent_path):
        """
            Find the absolute path of the lustre files, since lustre changelog does not
            include the root directory of the file in the changelog records
        """
        if not parent_path: parent_path = ""
        if not target_path: target_path = ""
        if not parent_path: parent_path = ""

        # Changelog acts weired when recording the target file and the related paths
        if target_file:
            if target_file in target_path:
                file_dir_path = target_path
            else:
                file_dir_path = (parent_path + "/" if parent_path else "") + target_file
        elif target_path:
            if "File Not Exist" == target_path:
                file_dir_path = parent_path if parent_path else "Changelog didn't Record the Path"
            else:
                file_dir_path = target_path
        elif parent_path:
            file_dir_path = parent_path

        else:
            file_dir_path = "Changelog didn't Record the Path"

        #---- TODO: Has to be modified ----
        #... find_absolute_path(file_dir_path)

        return file_dir_path


class ProvenanceShell(Cmd):
    """
        The interactive shell interface for Provenance Client. This program
        communicates with Provenance-RESTFUl-API and processes sys admin's
        commands inclduing:
            - Demonstrate OSS/OST Jobstats info
            - Demonstrate MDS/MDT Jobstats info
            - Files and file operations per MDT/JobInfo
            - I/O and File Operations per Jobs
    """
    def __init__(self):
        super(ProvenanceShell, self).__init__()
        # The Introduction Header
        self.intro = "|=============================================|\n" \
                     "|   Provenance Command Line Interface v.1.0   |\n" \
                     "|      High Performance Computing Center      |\n" \
                     "|            Texas Tech University            |\n" \
                     "|=============================================|\n"

        # Command Prompt format
        self.prompt = "Provenance> "
        # The Help Header
        self.doc_header = "(Help):"
        # Choose the ruler character
        self.ruler = "-"
        # The function when Ctrl+D is called
        self.do_EOF = self.do_exit
        # Main RestAPI req/resp object
        self.rest_api = RESTful_API()

        # Get the Lustre Schema in the DataCenter
        self.lustre_schema : dict = self.rest_api.get("/schema").get("schema", None)
        # If Schema is empty, it means something is wrong on the API Server
        if not self.lustre_schema:
            print("The Lustre Schema is empty.")
            self.do_exit()

        # Check the schema if it's wrong
        if not {"oss", "mds"}.issubset(set(self.lustre_schema)):
            print("The 'Lustre Schema' is wrong or incompelete.")
            self.do_exit()

        # Create a Session for thi Interactive Shell
        self.session = self.Session()

    def do_test(self, arg):
        # response = self.rest_api.get("/schema")
        # print(response)
        print(str(arg))

    def help_show(self):
        help_msg = """
NAME:
    show - Get a list of OSS(s)/MDS(s) servers abd their OST(s)/MDT(s) targets

SYNOPSIS
    show
    show [servers | [targets]]  

DESCRIPTION
    This command prints a list of available Lustre servers (OSS/MDS) that can be selected by
    'select' command. It will also show the server targets (by adding 'target' option) once 
    an OSS or MDS server is selected. If no servers is selected, 'show target' cannot show
    a list of corresponding targets.

EXAMPLES
    Provenance> show
        List of available servers that can be selected:
         - MDS Servers:
           +- mgs

         - OSS Servvers:
           +- oss1
           +- oss2

    Provenance> select oss1
    Provenance [oss1]> show targets
        List of available targets in (oss1):
         +- test-OST0001
            """
        print(help_msg)

    def do_show(self, arg):
        """
            Show all the available servers and targets: mds | oss | Targets
        """
        # Parse Arguments
        args = self._parse_arg(arg)
        # To many arguments
        if len(args) > 1:
            self._error("Too many arguments for 'show'")
            return

        option = args[0] if len(args) == 1 else None
        # List Servers:
        if (len(args) < 1) or (option == "servers"):
            output_list = " List of available servers that can be selected:\n"
            # Iterate over MDS Servers
            output_list += "\n - MDS Servers:\n"
            for mds_server in self.lustre_schema.get('mds').keys():
                output_list += f"   +- {mds_server}\n"
            # Iterate over OSS Servers
            output_list += "\n - OSS Servvers:\n"
            for oss_server in self.lustre_schema.get('oss').keys():
                output_list += f"   +- {oss_server}\n"

        # List Targets:
        elif option == "targets":
            # Cannot List Targets out of SERVER and TARGET mode
            if self.session.mode not in [self.Mode.SERVER, self.Mode.TARGET]:
                self._error("No server has been selected")
                return
            output_list = f" List of available targets in ({self.session.serverName}):\n"
            for target in self.session.targetList:
                output_list += f"   +- {target}\n"

        # Else, invalid option
        else:
            self._error(f"'{option}' is not a valid option for 'show' command\n"
                        f" + show\n"
                        f" + show <servers|targets>")
            return

        # Print the show
        print(output_list)


    def help_select(self):
        help_msg = """
NAME
    select - select OSS/MDS server, OST/MDT target if applicable, or jobs
    
SYNOPSIS
    select [server | [target]] server/target
    select server
    select jobs
    
DESCRIPTION
    Select command has two modes: 1) 'Server/Target' mode 2) 'Jobs' mode. User can select a lustre server 
    (i.e. OSS or MDS) or the 'jobs' mode at any moment. But can only select a Target only once a 'server' 
    is selected. the 'list' command works only when a Server, Target, or 'Jobs' mode is selected.
    
EXAMPLE
     Provenance> select server oss1
     Provenance [oss1]> select oss2
     Provenance [oss2]> select target test-OST0001
     Provenance [oss1]->(test-OST0001)> select oss1
     Provenance [oss1]> select jobs
     Provenance [jobs]> 
        """
        print(help_msg)

    def do_select(self, arg):
        """
            Select a Server, Target or Jobs mode
        """
        # Parse Arguments
        args = self._parse_arg(arg)
        # At least one argument
        if len(args) < 1:
            self._error("'select' command requires at least one argument\n"
                        "  + select <server|target> arg\n"
                        "  + select {server}\n"
                        "  + select jobs")
            return
        # Too many arguments
        if len(args) > 2:
            self._error("Too many arguments for 'select'\n"
                        "  + select <server|target> arg\n"
                        "  + select {server}\n"
                        "  + select jobs")
            return

        # ----------- SERVER -----------
        if (len(args) == 1 and args[0] != 'jobs') or (args[0] == 'server' if len(args) > 1 else None):

            server = args[1] if len(args) > 1 else args[0]
            # Find the Type of the selected server
            if server in self.lustre_schema.get('mds').keys():
                serverType = 'mds'
            elif server in self.lustre_schema.get('oss').keys():
                serverType = 'oss'
            else:
                # Ignore Wrong Argument
                self._error(f"'{server}' is not a correct server name")
                return

            targetList = self.lustre_schema.get(serverType, {}).get(server, None)

            # Update current session
            self.session.updateServerMode(serverType, server, targetList)

        #----------- TARGET ------------
        elif args[0] == 'target':
            if len(args) == 1:
                self._error("'select target' has not enought arguments: [select target <TARGET>]")
                return
            # The current state hast to be in SERVER or TARGET mode
            if self.session.mode not in [self.Mode.SERVER, self.Mode.TARGET]:
                self._error(f"No Server has been selected for ({args[1]}) target")
                return

            # Get the list of available targets:
            serverName = self.session.serverName
            targetList = self.session.targetList
            target = args[1]

            # Ignore Wrong Argument
            if target not in targetList:
                self._error(f"'{target}' is not available in ({serverName}) targets")
                return

            # Update Current Session
            self.session.updateTargetMode(target)

        # -------- JOBS -------------
        elif args[0] == 'jobs':
            # job has no argument
            if len(args) > 1:
                self._error("'select jobs' has no arguments")
                return
            # Update the current session
            self.session.updateJobsMode()

        #------------ INVALID -----------
        else:
            self._error(f"'{args[0]}' is not a valid option for 'select' command\n"
                        f"  + select <server|target> arg\n"
                        f"  + select serverName\n"
                        f"  + select jobs")
            return
        # Finally, update the command prompt
        self._update_prompt()


    def do_list(self, args):
        """
            Query and Get requested data from Provenance API, and print the output
            in a tabulate format
        """
        # 'list command requires at least a server or 'jobs' to be selected'
        if self.session.mode == self.Mode.ROOT:
            self._error("Please select a server, server/target/ or jobs")
            return
        # Parse Arguments
        args_map = self._pars_list_args(args)
        if args_map.get("error", None):
            self._error(args_map["error"])
            return

        list_cmd = args_map.pop('cmd')
        #--------------- Server Mode -----------------
        if self.session.mode in [self.Mode.SERVER, self.Mode.TARGET]:
            # # Make a header
            # header = f" {self.session.serverType} Server: [{self.session.serverName}] \n" + \
            #     (f"\n Target: [{self.session.targetName}] \n    " if self.session.targetName else "")
            #************ OSS Servers **********
            if self.session.serverType == "oss":
                # list cannot get file ops on OSSs
                if list_cmd == "files":
                    self._error("File operations are not available on OSS servers")
                    return

                elif list_cmd == "jobs":
                    data_table = self.rest_api.get_oss_jobs_table(self.session.serverName,
                                                                  target=self.session.targetName, **args_map)

                    self._paginate_output(data_table)

            # ************ MDS Servers **********
            if self.session.serverType == "mds":
                if list_cmd == "jobs":
                    data_table = self.rest_api.get_mds_jobs_table(self.session.serverName,
                                                                  target=self.session.targetName, **args_map)
                    self._paginate_output(data_table)
                    
                elif list_cmd == "files":
                    if self.session.mode != self.Mode.TARGET:
                        self._error("Please select a MDT target to list available file operations")
                        return

                    # Request for files:
                    args_map.update({'files': 'y'})
                    data_table = self.rest_api.get_mds_files_Table(self.session.serverName,
                                                                  target=self.session.targetName, **args_map)
                    self._paginate_output(data_table)

        #--------------- Jobs Mode -----------------
        elif self.session.mode ==  self.Mode.JOBS:

            if list_cmd == "files":
                self._error("File Operations will appear once a single job is listed for details")
                return

            elif list_cmd == "jobs":
                if "jobid" in args_map.keys():
                    data_table = self.rest_api.get_job_detail(args_map.get("jobid"), args_map.get("taskid", None))
                else:
                    data_table = self.rest_api.get_all_jobs(**args_map)

                self._paginate_output(data_table)


    def help_back(self):
        help_msg = """
NAME
    back - return one level back up
    
SYNOPSIS
    back
    
DESCRIPTION
    The 'back' command changes the current session mode to one level back. For instance, 
    if a 'target' is selected, then 'back' will return the session back to 'server' mode.
    And if 'server' or 'jobs' are selected, then session will return to it's root condition.
    
EXAMPLE
    Provenance [oss1]->(test-OST0001)> back
    Provenance [oss1]> back
    Provenance> 
        """
        print(help_msg)

    def do_back(self, arg):
        """
            Return one level back up
        """
        if self.session.mode == self.Mode.TARGET:
            self.session.updateServerMode(self.session.serverType,
                                          self.session.serverName,
                                          self.session.targetList)
        else:
            self.session.setToRoot()
        self._update_prompt()


    def do_help(self, arg: str):
        """ (help): Show the help info for the given command"""
        if not arg:
            print("This is the help")
        else:
            # Once the arg was defined, then show the help for arg (command)
            super().do_help(arg)

    def do_exit(self, noarg):
        """
            exit:
                Exit Provenance Cli Shell
        """
        print("Bye!")
        return True

    def default(self, inp: str) -> bool:
        self._error(f"The '{inp.split()[0]}' is not a valid command")
        return False

    @staticmethod
    def _paginate_output(output):
        """
            Paginate the long outputs with "less" command
        """
        # The less command that only paginate if number of output lines are larger
        # than the screen size. It also chop the lines to fit in one line only
        pager = Popen(['less', '-F', '-R', '-S', '-X', '-K'], stdin=PIPE)
        # Pipe the output to less
        pager.stdin.write(f"\n{output}".encode('utf-8'))
        # flush and send (EOF) before calling wait
        pager.stdin.close()
        # Wait for "less"
        pager.wait()

    @staticmethod
    def _parse_arg(arg: str):
        """
            Parse the argument that is passed to each command
        """
        ars_list = []
        if arg:
            # Split arguments and convert them to lowercase
            for item in arg.strip().split():
                ars_list.append(item)
        return ars_list

    @staticmethod
    def _pars_list_args(arg: str):
        arg_map = OrderedDict()
        # A map of valid arguments that work with list command
        valid_ops = {"jobid": ['-j', '--jobid'], "taskid": ['-t', '--taskid'], "sort": ['-s', '--sort'],
                      "js": ['-js', '--job-status'],  "days": ['-d', '--days']}
        # List of valid commands for 'list'
        valid_cmds = ['jobs', 'files']
        # Convert args to list
        arg_list = arg.strip().split()
        # 'list' requires at least one command
        if not len(arg_list):
            return {"error": f"'list' requires at least one command to execute"}
        # Check if the command is valid
        if arg_list[0] not in valid_cmds:
            return {"error": f"The '{arg_list[0]}' is not a valid command for 'list'"}
        # Add command
        arg_map.update({"cmd": arg_list[0]})
        # Get all options
        ops_list = arg_list[1:]
        # collect arg_map
        for inx, _ in enumerate(ops_list):
            option = ops_list[inx]
            # Expected to be option:
            if not inx % 2: # Odd arguments (even index!)

                if not option.startswith('-'):
                    return {"error": f"The '{option}' is not a valid option for 'list'"}
                # Check if option is valid
                for key, value in valid_ops.items():
                    if option in value:
                        arg_map.update({key: None})
                        break
                else:
                    return {"error": f"The '{option}' is not a valid option for 'list'"}

            else: # Even arguments must be values
                last_key = list(arg_map.keys())[-1]
                last_option = ops_list[inx-1]
                if option.startswith('-'):
                    return {"error": f"The '{option}' is not a valid value for '{last_option}'"}
                arg_map.update({last_key: option})

        # Make sure all the items have a value:
        for key, value in arg_map.items():
            if not value:
                return {"error": f"The '{ops_list[(list(arg_map.keys()).index(key) - 1) * 2]}' option has no value"}
        # Return arg_map
        return arg_map

    @staticmethod
    def _error(msg: str):
        """
            Print formated Error message
        """
        print(f"\n Error: {msg}\n")

    def _update_prompt(self):
        """
            Updating the command prompt format
        """
        prompt = "Provenance"
        if self.session.mode == self.Mode.JOBS:
            prompt += " [jobs]"
        elif self.session.mode == self.Mode.SERVER:
            prompt += f" [{self.session.serverName}]"
        elif self.session.mode == self.Mode.TARGET:
            prompt += f" [{self.session.serverName}]->"
            prompt += f"({self.session.targetName})"
        prompt += "> "
        self.prompt = prompt
        
    
    class Session(object):
        """
            A Class for keep tracking the current state of the session
        """
        def __init__(self):
            self.mode = ProvenanceShell.Mode.ROOT
            self.serverType = None
            self.serverName = None
            self.targetName = None
            self.targetList = []

        def updateServerMode(self, stype, sname, targetlist):
            self.mode = ProvenanceShell.Mode.SERVER
            self.serverType = stype
            self.serverName = sname
            self.targetName = None
            self.targetList = targetlist

        def updateTargetMode(self, target):
            self.mode = ProvenanceShell.Mode.TARGET
            self.targetName = target

        def updateJobsMode(self):
            self.mode = ProvenanceShell.Mode.JOBS
            self.serverType = None
            self.serverName = None
            self.targetName = None
            self.targetList = []

        def setToRoot(self):
            self.mode = ProvenanceShell.Mode.ROOT
            self.serverType = None
            self.serverName = None
            self.targetName = None
            self.targetList = []

    

    class Mode(Enum):
        """
            Different available states
        """
        ROOT = 0
        SERVER = 1
        TARGET = 2
        JOBS = 3

        @classmethod
        def getState(cls, arg: str):
            if arg in {'oss', 'mds'}:
                return cls.SERVER
            elif arg == 'jobs':
                return cls.JOBS
            else:
                return cls.TARGET



if __name__ == '__main__':
    try:
        ProvenanceShell().cmdloop()
    except KeyboardInterrupt:
        print("Bye!")