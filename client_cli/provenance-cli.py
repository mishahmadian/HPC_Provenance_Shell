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
from getopt import getopt, GetoptError
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
CLUSTERS_SCHED = {
    "genius": "uge",
    "quanah": "uge",
    "hrothgar": "uge"
}
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
            # Waiting Notification
            print(f" {Color.ORANGE}Please Wait... \r{Color.NO_COLOR}", end='', flush=True)
            # Generate the proper request URL
            req_url = self._gen_api_url(url_cmd, **kwargs)
            # Send request and get response
            response = urllib.request.urlopen(req_url)
            # Clear Waiting notification
            print(" " * 20, end='\r')
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

        except urllib.error.URLError:
            print(" " * 20, end='\r')
            print("\n [Error] Provenance Cli cannot connect to the API server\n")
            sys.exit(1)

        except Exception as exp:
            print(" " * 20, end='\r')
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
            headers = ["Cluster", "JobID", "TaskID", "User", "Stauts", "Read OPs", "Total Read Size",
                       "Write OPs", "Total Write Size"]
            if not target: headers.append("OST")
            table = []
            for data in results:
                row = [data["cluster"], data["jobid"], data["taskid"], data["username"], data["status"],
                       data["oss_info"]["read_bytes"], self._convert_from_byte(data["oss_info"]["read_bytes_sum"]),
                       data["oss_info"]["write_bytes"], self._convert_from_byte(data["oss_info"]["write_bytes_sum"])]

                if not target: row.append(data["oss_info"]["ost_target"])
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
            headers = ["Cluster", "JobID", "TaskID", "User", "Stauts", "Open/Close", "Get/Set Attr",
                       "rm/unlink", "mkdir/rmdir", "statfs"]

            if not target: headers.append("MDT")
            table = []
            for data in results:
                row = [data["cluster"], data["jobid"], data["taskid"], data["username"], data["status"],
                       f"{data['mds_info']['open']}/{data['mds_info']['close']}",
                       f"{data['mds_info']['getattr']}/{data['mds_info']['setattr']}",
                       f"{data['mds_info']['unlink']}",
                       f"{data['mds_info']['mkdir']}/{data['mds_info']['rmdir']}",
                       f"{data['mds_info']['statfs']}"]

                if not target: row.append(data['mds_info']['mdt_target'])
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
            headers = ["cluster", "JobID", "TaskID", "User", "Stauts", "File/Dir OP", "File/Dir Path"]
            table = []
            for data in results:
                # Replace the type UNLNK with REMOVE which is more meaningful
                if data["fileop_info"]["file_ops"]["op_type"] == "UNLNK":
                    data["fileop_info"]["file_ops"]["op_type"] = "REMOVE"

                row = [data["cluster"], data["jobid"], data["taskid"], data["username"], data["status"],
                       data["fileop_info"]["file_ops"]["op_type"]]

                # Find the absolute path of the file or directory
                file_dir_path = self._find_absolute_path(data["fileop_info"]["target_file"],
                                                         data["fileop_info"]["target_path"],
                                                         data["fileop_info"]["parent_path"])

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
            # headers = ["Cluster", "JobID", "TaskID", "JobName", "User", "Stauts", "Project", "PE", "# CPU",
            #            "Submitted", "Started", "Finished"]
            headers = ["Cluster", "Scheduler", "JobID", "TaskID", "JobName", "User", "Stauts", "# CPU",
                       "Submitted", "Started", "Finished"]

            table = []
            for data in results:
                # row = [data["cluster"], data["jobid"], data["taskid"], data["jobName"], data["username"],
                #        data["status"], data["project"], data["parallelEnv"], data["num_cpu"]]
                row = [data["cluster"], data["sched_type"], data["jobid"], data["taskid"], data["jobName"], data["username"],
                       data["status"], data["num_cpu"]]

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

    def get_job_detail(self, cluster, sched, jobid, taskid, **kwargs):
        """
            Request API and get all the details regarding a job
        """
        # Calculate the uid for the job
        uid = self._uniqID(cluster, sched, jobid, taskid)
        url_cmd = f"/jobinfo/{uid}"
        # Call the Rest API
        api_response = self.get(url_cmd)
        # Get Query results
        results = api_response.get("result", None) if api_response else {}

        if results and results[0]:
            data = results[0]
            data_table = f"{Color.CYAN}[JOB INFO]{Color.NO_COLOR}\n\n"
            # headers = ["Cluster", "JobID", "TaskID", "JobName", "User", "Stauts", "Project", "PE", "# CPU",
            #            "Submitted", "Started", "Finished"]
            headers = ["Cluster", "Scheduler", "JobID", "TaskID", "JobName", "User", "Stauts", "# CPU",
                       "Submitted", "Started", "Finished"]

            #--------------- JOB INFO 1st part ------------------
            # row = [data["cluster"], data["jobid"], data["taskid"], data["jobName"], data["username"], data["status"],
            #        data["project"], data["parallelEnv"], data["num_cpu"]]
            row = [data["cluster"], data["sched_type"], data["jobid"], data["taskid"], data["jobName"], data["username"]
                  , data["status"], data["num_cpu"]]

            # Convert epoch timestamps to readable date/time format
            for timestamp in [data["submit_time"], data["start_time"], data["end_time"]]:
                if timestamp:
                    row.append(datetime.fromtimestamp(float(timestamp) / 1000.0).strftime("%m.%d.%Y\n%H:%M:%S"))
                else:
                    row.append("N/A")

            data_table += tabulate([row], headers=headers, tablefmt="simple", stralign="center", numalign="center")

            # --------------- JOB INFO 2nd part ------------------
            headers = ["Parameters", "Values", "Parametes", "Values"]
            col1 = [["Queue/Partition", data["queue"]], ["Mem_Core", data["h_vmem"]], ["Hard Runtime", data["h_rt"]],
                    ["Soft Runtime", data["s_rt"]]]

            col2 = [["Working Directory", data["pwd"]], ["Command", data["command"]],
                    ["Deleted/Canceled", ', '.join(filter(None, data["q_del"]))],
                    ["Failed", ("yes" if data["failed_no"] else "No")]]

            two_col_tbl = [c1+c2 for c1,c2 in zip(col1, col2)]
            data_table += "\n" + tabulate(two_col_tbl, headers=headers, tablefmt="psql",
                                          stralign="left", numalign="left") + "\n\n"

            # --------------- MDS Dynamic Table ------------------
            # Check and see if MDS data should be included
            if not kwargs or kwargs.get("mds", False):
                mds_list = data["mds_data"]
                mds_map = {}

                # Map all the MDS Data into their MDS Server
                for mdsData in mds_list:
                    mds = mdsData["mds_info"].pop("mds_host")
                    if mds in mds_map.keys():
                        mds_map[mds].append(mdsData.get("mds_info"))
                    else:
                        mds_map[mds] = [mdsData.get("mds_info")]

                # Create MDS Tables
                for mds, mdsDataLst in mds_map.items():
                    data_table += f"{Color.PURPLE}[MDS: {mds}]{Color.NO_COLOR}\n"
                    headers = [title for inx in range(len(mdsDataLst))
                                for title in [f"({mdsDataLst[inx].pop('mdt_target')})", "Stats"]]

                    mdt_table = []
                    for mdsData in mdsDataLst:
                        # convert timestamp to Date/Time
                        mdsData["modified_time"] = datetime.fromtimestamp(
                            float(mdsData["modified_time"])
                        ).strftime("%m-%d-%Y\n%H:%M:%S")

                        col = [[key, str(value)] for key, value in mdsData.items() if key != "file_op"]
                        mdt_table.append(col)

                    # Create MDT Table
                    mds_table = list(map(lambda x: list(chain(*x)), zip(*mdt_table)))

                    data_table += "\n" + tabulate(mds_table, headers=headers, tablefmt="psql",
                                                  stralign="left", numalign="left") + "\n\n"

            # --------------- OSS Dynamic Table ------------------
            # Check and see if OSS data should be included
            if not kwargs or kwargs.get("oss", False):
                oss_list = data["oss_data"]
                oss_map = {}

                # Map all the OSS Data into their OSS Server name
                for ossData in oss_list:
                    oss = ossData["oss_info"].pop("oss_host")
                    if oss in oss_map.keys():
                        oss_map[oss].append(ossData.get("oss_info"))
                    else:
                        oss_map[oss] = [ossData.get("oss_info")]

                # Create OSS Tables
                for oss, ossDataLst in oss_map.items():
                    data_table += f"{Color.ORANGE}[OSS: {oss}]{Color.NO_COLOR}\n"

                    headers = [title for inx in range(len(ossDataLst))
                                for title in [f"({ossDataLst[inx].pop('ost_target')})", "Stats"]]
                    ost_table = []
                    for ossData in ossDataLst:
                        # convert timestamp to Date/Time
                        ossData["modified_time"] = datetime.fromtimestamp(
                            float(ossData["modified_time"])
                        ).strftime("%m-%d-%Y\n%H:%M:%S")

                        # Change some titles
                        ossData["read_bytes"] = f"{ossData['read_bytes']} OPs"
                        ossData["write_bytes"] = f"{ossData['write_bytes']} OPs"

                        col = [[key, self._convert_from_byte(value) if "_bytes_" in key else str(value)]
                                    for key, value in ossData.items()]
                        ost_table.append(col)

                    # Create OST Table
                    oss_table = list(map(lambda x: list(chain(*x)), zip(*ost_table)))

                    data_table += "\n" + tabulate(oss_table, headers=headers, tablefmt="psql",
                                                  stralign="left", numalign="left") + "\n\n"

            # --------------- Files and File OPs per Jobs ------------------
            # Check and see if File OPs data should be included
            if not kwargs or kwargs.get("files", False):
                # Map The File OPs to their MDT target
                fileop_map = {}
                for fileObj in data.get("fileop_data", []):
                    if fileObj["mdtTarget"] in fileop_map:
                        fileop_map[fileObj.pop("mdtTarget")].append(fileObj)
                    else:
                        fileop_map[fileObj.pop("mdtTarget")] = [fileObj]

                # Create File OP Table
                for mdt_target, fileop_lst in fileop_map.items():
                    data_table += f"{Color.RED}[Files OPs: {mdt_target}]{Color.NO_COLOR}\n"
                    headers = ["Date/Time", "Node", "OP", "Mode", "File/Dir Path"]
                    table = []
                    for fileobj in fileop_lst:
                        row = [
                            datetime.fromtimestamp(int(fileobj["file_ops"]["timestamp"])).strftime("%m.%d.%Y %H:%M:%S"),
                            fileobj["nid"] if fileobj["nid"] else "N/A",
                            fileobj["file_ops"]["op_type"] if fileobj["file_ops"]["op_type"] != "UNLNK" else "REMOVE",
                            fileobj["file_ops"]["open_mode"],
                            self._find_absolute_path(fileobj["target_file"],
                                                     fileobj["target_path"],
                                                     fileobj["parent_path"])
                        ]
                        table.append(row)

                    #Make table:
                    data_table += "\n" + tabulate(table, headers=headers, tablefmt="simple",
                                                  stralign="left", numalign="left") + "\n"

            # Print
            return data_table


        # Otherwise Print no output data
        return "** No Result Found **"


    def get_jobscript(self, cluster, jobid):
        """
            Request API and get a job submission script for a particular job
        """
        # Calculate the uid for the job
        url_cmd = f"/jobscript/{cluster}/{jobid}"
        # Call the Rest API
        api_response = self.get(url_cmd)
        # Get Query results
        result = api_response.get("result", None) if api_response else {}

        if result:

            return f"{Color.PURPLE}[Job Submission Script]\n" \
                   f"{Color.ORANGE}JobId: ({result['jobid']})  Cluster: ({result['cluster']})\n\n" \
                   f"{Color.NO_COLOR}{result['job_script']}"

        # Otherwise Print no output data
        return "** The Job Submission Script was not found **"


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
            cluster = kwargs.pop('cluster') if kwargs.get('cluster', None) else None
            sched = kwargs.pop('sched') if kwargs.get('sched', None) else None
            # Generate UID and update args
            if jobid:
                uid = self._uniqID(cluster, sched, jobid, taskid)
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
    def _uniqID(cluster, sched, jobid, taskid=None):
        """
        Generate the 'uid' out of jobid and taskid
        """
        global CLUSTERS_SCHED
        # calculate the MD5 hash
        #sched = CLUSTERS_SCHED.get(cluster, None)
        obj_id = ''.join(filter(None, [sched, cluster, jobid, taskid]))
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
                     "|      Provenance Command Line Interface      |\n" \
                     "|                   v.1.0                     |\n" \
                     "|      High Performance Computing Center      |\n" \
                     "|        Department of Computer Science       |\n" \
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


    def help_show(self):
        help_msg = """
NAME:
    show - Get a list of OSS(s)/MDS(s) servers and their OST(s)/MDT(s) targets

SYNOPSIS
    show
    show [servers | [targets]]  

DESCRIPTION
    This command prints a list of available Lustre servers (OSS/MDS) that can be selected by
    'select' command. It will also show the server targets (by adding 'target' option) once 
    an OSS or MDS server is selected. If no servers is selected, 'show target' cannot show
    the list of corresponding targets.

EXAMPLES
    Provenance> show
        List of available servers that can be selected:
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
    select - select OSS/MDS server, (or OST/MDT target if applicable), or cluster jobs
    
SYNOPSIS
    select [server | [target]] <server_name>|<target_name>
    select <server_name>|<target_name>
    select jobs
    
DESCRIPTION
    Select command has three modes: 1) 'Server' mode 3) 'Target' mode, and 3) 'Jobs' mode. User can select 
    a lustre server (i.e. OSS or MDS) or the 'jobs' mode at any moment. But can only select a Target only  
    once a 'server' is selected. the 'jobs' command works only when a Server, Target, or 'Jobs' mode is 
    selected.
    
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
        #
        # Set the OSS/MDS server and modify the session
        #
        def set_server(server_name):
            # nonlocal args
            # server = args[1] if len(args) > 1 else args[0]
            # Find the Type of the selected server
            if server_name in self.lustre_schema.get('mds').keys():
                serverType = 'mds'
            elif server_name in self.lustre_schema.get('oss').keys():
                serverType = 'oss'
            else:
                # Ignore Wrong Argument
                self._error(f"'{server_name}' is not a correct server name")
                return

            targetList = self.lustre_schema.get(serverType, {}).get(server_name, None)

            # Update current session
            self.session.updateServerMode(serverType, server_name, targetList)
        #
        # Set the OSS/MDT target and modify the session
        #
        def set_target(target_name):
            #nonlocal args
            # if len(args) == 1:
            #     self._error("'select target' has not enought arguments: [select target <TARGET>]")
            #     return
            # The current state hast to be in SERVER or TARGET mode
            if self.session.mode not in [self.Mode.SERVER, self.Mode.TARGET]:
                self._error(f"No Server has been selected for ({target_name}) target")
                return

            # Get the list of available targets:
            serverName = self.session.serverName
            targetList = self.session.targetList

            # Ignore Wrong Argument
            if target_name not in targetList:
                self._error(f"'{target_name}' is not a MDT target of ({serverName}) server")
                return

            # Update Current Session
            self.session.updateTargetMode(target_name)
        #
        # The Main Section
        #
        # Parse Arguments
        args = self._parse_arg(arg)
        # At least one argument
        if len(args) < 1:
            self._error("'select' command requires at least one argument\n"
                        "  + select <server|target> arg\n"
                        "  + select {server/target}\n"
                        "  + select jobs")
            return
        # Too many arguments
        if len(args) > 2:
            self._error("Too many arguments for 'select'\n"
                        "  + select <server|target> arg\n"
                        "  + select {server/target}\n"
                        "  + select jobs")
            return

        # ----------- No arg: Server/Target -----------
        if len(args) == 1 and args[0] not in ['server', 'target', 'jobs']:
            select_arg = args[0]
            # Check if The selected argument exists among OSS/MDS servers
            if select_arg in self.lustre_schema.get('mds').keys() or \
                select_arg in self.lustre_schema.get('oss').keys():
                # Then select it as a server
                set_server(select_arg)
            # Check if The selected argument exists among current OSTs/MDSs
            elif select_arg in self.session.targetList:
                # Then select it as target
                set_target(select_arg)
            else:
                self._error(f"The '{select_arg}' is not a server nor an available target")
                return

        # ----------- SERVER -----------
        elif args[0] == 'server':
            if len(args) == 1:
                self._error("'select server' has not enought arguments: [select server <server_name>]")
                return
            set_server(args[1])

        #----------- TARGET ------------
        elif args[0] == 'target':
            if len(args) == 1:
                self._error("'select target' has not enought arguments: [select target <target_name>]")
                return
            set_target(args[1])

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


    def help_jobs(self):
        help_msg = """
NAME
    jobs - lists all the jobs for current selected OSS/MDS server ot OST/MDT Target
    
SYNOPSIS
    jobs [-jtcusdf][-js] content
    jobs [--jobid][--taskid][--cluster][--username][--job-status][--files][--days][--sort] content
    
DESCRIPTION
    Once an OSS/MDS server or a MDT/OST target is selected, the 'jobs' command can list all the
    jobs for that particular OSS/OST or MDS/MDT. By defualt, the 'jobs' command lists all the 
    current RUNNING jobs on these servers/targets. If a MDT/OST is selected, then list of the jobs 
    will be filtered for that selected target.
    
OPTIONS
    
    -j, --jobid <job_id>        Shows the record of a particular job with <job_id>
    
    -t, --taskid <task_id>      Shows the record of this particular array job with <job_id>, <task_id>
                                Once the <task_id> was specified, the <job_id> must be defined as well.
                                (Note that the <job_id> alone will never show an array job without taskid)
    
    -c, --cluster <cluster>     Filters the jobs output for a particular <cluster>. Selecting this option
                                is mandatory when -j is selected.
                                
    -u, --username <username>   Shows all the records that belong to a particualr user.
                                
    -js, --job_status <status>  Search for the jobs in RUNNING or FINISHED states. <status> can be defined 
                                as ['r', 'R'] for RUNNING jobs or ['f', 'F'] for FINISHED jobs.
    
    -f, --file                  Lists all the files. This option is only available on MDT mode.
    
    -d, --days <N>              Lists all the records fo N days ago.
    
    -s, --sort <filed>          Sorts the list based on the filed name. 
                                ** (This option is not fully functional at this time)
        """
        print(help_msg)

    def do_jobs(self, args):
        """
            Query and Get requested data from Provenance API, and print the output
            in a tabulate format
        """
        # 'list command requires at least a server or 'jobs' to be selected'
        if self.session.mode == self.Mode.ROOT:
            self._error("Please select a server, server/target/ or jobs")
            return
        # Parse Arguments
        args_map = self._pars_jobs_args(args)
        if args_map.get("error", None):
            self._error(args_map["error"])
            return

        has_files = 'files' in args_map
        #--------------- Server Mode -----------------
        if self.session.mode in [self.Mode.SERVER, self.Mode.TARGET]:
            # # Make a header
            # header = f" {self.session.serverType} Server: [{self.session.serverName}] \n" + \
            #     (f"\n Target: [{self.session.targetName}] \n    " if self.session.targetName else "")
            #************ OSS Servers **********
            if self.session.serverType == "oss":
                # list cannot get file ops on OSSs
                if has_files:
                    self._error("Files can be shown only for MDT targets on MDS servers")
                    return
                else:
                    data_table = self.rest_api.get_oss_jobs_table(self.session.serverName,
                                                                  target=self.session.targetName, **args_map)

                    self._paginate_output(data_table)

            # ************ MDS Servers **********
            if self.session.serverType == "mds":
                if has_files:
                    if self.session.mode != self.Mode.TARGET:
                        self._error("Please select a MDT target to list available file operations")
                        return

                    data_table = self.rest_api.get_mds_files_Table(self.session.serverName,
                                                                  target=self.session.targetName, **args_map)
                    self._paginate_output(data_table)

                else:
                    data_table = self.rest_api.get_mds_jobs_table(self.session.serverName,
                                                                  target=self.session.targetName, **args_map)
                    self._paginate_output(data_table)

        #--------------- Jobs Mode -----------------
        elif self.session.mode ==  self.Mode.JOBS:

            if has_files:
                self._error("Files can be shown only for MDT targets on MDS servers")
                return

            else:
                data_table = self.rest_api.get_all_jobs(**args_map)
                self._paginate_output(data_table)


    def help_jobinfo(self):
        help_msg = """
NAME
    jobinfo - returns all the provenance information of a particular job
    
SYNOPSIS
    jobinfo -c <cluster> -j <job_id> [-t <task_id>] [-m | -o | -f]
    
    jobinfo --cluster <cluster> --jobid <job_id> [--taskid <task_id>] [--mds | --oss | --files]
    
DESCRIPTION
    The 'jobinfo' command collects all the provenance information of a job including
    the I/O statistics of the job on OSSs/OSTs and metadata activities on MDSs/MDTs.
    It also provides some information regarding the job scheduler and lists all the
    File Operations that have been done by the selected job.
      * Defining the cluster (-c , --cluster) is mandatory
      * Please also notice that it is necessary to select the <task_id> if the job
        is an array job.
           
OPTIONS
    
    -j, --jobid <job_id>        Shows provenance data of a particular job with <job_id> (Mandatory)
    
    -t, --taskid <task_id>      Shows the Provenance of a particular array job with <job_id> <task_id>.
                                Once the <task_id> was defined, the <job_id> must be defined as well.
                                (Note that the <job_id> alone will never show an array job without taskid)
    
    -c, --cluster <cluster>     Select the Cluster Name which the desired job comes from
    
    -o, --oss                   Filters the output for OSS data
    
    -m, --mds                   Filters the output for OSS data
    
    -f, --files                 Filters the output for File Operations data
        """
        print(help_msg)


    def do_jobinfo(self, args):
        """
            Show all the details regarding a job
        """
        args_map = {'cluster': None, 'sched': None, 'jobid': None, 'taskid': None}
        flags_list = ["oss", "mds", "files"]
        filter_flags = {}
        arg_list = args.strip().split()
        # jobinfo needs at least one argument
        if not arg_list:
            self._error("Please choose the cluster_name, job_id (and a task_id if applicable).\n "
                        "For more information please refer to the help manual of 'jobinfo'")
        try:
            # Parse the arguments
            opts, remain = getopt(arg_list, 'c:s:j:t:omf', ['cluster', 'sched', 'jobid', 'taskid', 'oss', 'mds', 'files'])
            # No non-argument input
            if remain:
                self._error(f"The '{remain}' is not a valid option")
                return

            # Collect options and their values
            for opt, value in opts:
                for flag in flags_list:
                    if opt in [f"--{flag}", f"-{flag[0]}"]:
                        filter_flags[flag] = True
                        break

                else:
                    for key in args_map.keys():
                        if opt in [f"--{key}", f"-{key[0]}"]:
                            if not value:
                                self._error(f"Please specify a value for '{opt}' option")
                                return
                            args_map[key] = value
                            break

            # Cluster name is mandatory
            if not args_map['cluster']:
                self._error("The Cluster name (-c, --cluster) has to be defined for 'jobinfo' command")
                return

            # Scheduler is mandatory
            if not args_map['sched']:
                self._error("The Scheduler type (-s, --sched) has to be defined for 'jobinfo' command")
                return

            # The jobid has to be selected
            if not args_map['jobid']:
                self._error("The JobID (-j, --jobid) has to be defined for 'jobinfo' command")
                return

            # Get the JobInfo details from Provenance API
            data_table = self.rest_api.get_job_detail(args_map.get("cluster"),
                                                      args_map.get("sched"),
                                                      args_map.get("jobid"),
                                                      args_map.get("taskid", None),
                                                      **filter_flags)
            self._paginate_output(data_table)

        except GetoptError as getopExp:
            self._error(f"{getopExp}")


    def help_jobscript(self):
        help_msg = """
NAME
    jobscript - Shows the Job Submission Script of a particualr job
    
SYNOPSIS
    jobscript -c <cluster> -j <job_id> 
    jobscript --cluster <cluster> --jobid <job_id>
    
DESCRIPTION
    The 'jobscript' command retrieves the content of a user's job submission script file in order
    to allow system administrator to have a quick access to a particular job's script contents for
    any further evaluation. Please note that collecting the job submission scripts on Provenance 
    side falls into the following situations:
    
        1) Job runs long enought to allow Provenance Server locate the submitted job submission 
           script on the spool directory of the scheduler. This is the most reliable job script
           since it ensures no modification in the script file after the job submission.
          
        2) Job runs very shortly and Provenance Server never gets a chance to capture the job 
           script from the spool directory. In that case, the Provenance tries to locate the 
           job submission script on the user's directory and save it. This may not be reliable
           since user might have modified the file after the job submission.
           
        3) Job's life time is very short and user has removed the job submission script file so 
           far. Therefore, Provenance Server has never got a chance to capture the job submission
           script file to store it into the database.
           
    ** Please note that both -c and -j flags have to be defined to retrieve a job's script
    ** There is only one version of job submission script per JobID and array jobs do not have a 
       separate job submission script file.
        """
        print(help_msg)


    def do_jobscript(self, args):
        """
            Retrieve Users' Job Submission Script Content
        """
        args_map = {'cluster': None, 'jobid': None}
        arg_list = args.strip().split()
        # jobinfo needs at least one argument
        if not arg_list:
            self._error("Please choose the cluster_name and job_id for the 'jobscript' command.\n "
                        "For more information please refer to the help manual of 'jobscript'")
        try:
            # Parse the arguments
            opts, remain = getopt(arg_list, 'c:j:', ['cluster', 'jobid'])
            # No non-argument input
            if remain:
                self._error(f"The '{remain}' is not a valid option")
                return
            # Collect options and their values
            for opt, value in opts:
                for key in args_map.keys():
                    if opt in [f"--{key}", f"-{key[0]}"]:
                        if not value:
                            self._error(f"Please specify a value for '{opt}' option")
                            return
                        args_map[key] = value
                        break

            # Cluster name is mandatory
            if not args_map['cluster']:
                self._error("The Cluster name (-c, --cluster) has to be defined for 'jobscript' command")
                return

            # The jobid has to be selected
            if not args_map['jobid']:
                self._error("The JobID (-j, --jobid) has to be defined for 'jobscript' command")
                return

            output_result = self.rest_api.get_jobscript(args_map.get("cluster"), args_map.get("jobid"))

            self._paginate_output(output_result)

        except GetoptError as getopExp:
            self._error(f"{getopExp}")


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
        """
(help): Show the help info for the given command

    help <command>
        """
        if not arg:
            help_msg ="""
    Welcome to The PRovenance Command Line Interface (Shell). The Provenance Software Stack consists
    of several distributed components which collect I/O statistics and File Operations of Cluster jobs
    from different Lustre Servers (i.e. OSS/MDS) and after aggregate the data stores them into database.
    
    This Command line interface allows system administrators to trace down the jobs on OSS/MDS servers
    and provides all the details regarding the jobs' status, IOPs on OSS servers, metadata activity on
    MDS servers, and File Operations per job. 
    
    Available Commands are:
    
        - help       Shows this help message.
        - show       Shows a list of OSS(s)/MDS(s) servers and their OST(s)/MDT(s) targets.
        - select     Selects OSS/MDS server, OST/MDT target if applicable, or jobs.
        - jobs       Lists all the jobs for current selected OSS/MDS server ot OST/MDT Target.
        - jobinfo    Returns all the provenance information of a job.
        - jobscript  Retrieves the content of the job submission script for a particular job.
        - back       Returns one level back up in the server/target hierarchy.
        - exit       Exits the command line interface. 
        
    *** For more information regarding each command above, type 'help <command>' to get a help manual
            """
            print(help_msg)
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
    def _paginate_output(output, scroll_quit=True):
        """
            Paginate the long outputs with "less" command
            :param scroll_quit: automatically exit if the entire file can be displayed on the first screen
        """
        try:
            # The 'less' command that controls pagination
            paginate_cmd = ['less', '-R', '-S', '-X', '-K']
            # Let it quit pagination (scrolling) if entire content fits in the screen
            if scroll_quit:
                paginate_cmd.append('-F')
            # The less command that only paginate if number of output lines are larger
            # than the screen size. It also chop the lines to fit in one line only
            pager = Popen(paginate_cmd, stdin=PIPE)
            # Pipe the output to less
            pager.stdin.write(f"\n{output}\n\n".encode('utf-8'))
            # flush and send (EOF) before calling wait
            pager.stdin.close()
            # Wait for "less"
            pager.wait()
        except BrokenPipeError:
            pass

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
    def _pars_jobs_args(arg: str):
        arg_map = OrderedDict()
        # A map of valid arguments that work with list command
        valid_ops = {"jobid": ['-j', '--jobid'], "taskid": ['-t', '--taskid'], "cluster": ['-c', '--cluster'],
                     "js": ['-js', '--job-status'],  "days": ['-d', '--days'], "user": ['-u', '--username'],
                     "sort": ['-s', '--sort']}

        valid_status = {"RUNNING": ['r', 'R'], "FINISHED": ['f', 'F']}
        # Convert args to list
        arg_list = arg.strip().split()

        # The -f, --files has no argument and can be considered as a command
        for inx, opt in enumerate(arg_list):
            if opt in ['-f', '--files']:
                arg_list.pop(inx)
                arg_map.update({'files': 'y'})

        # collect all other arguments and create arg_map
        for inx, _ in enumerate(arg_list):
            option = arg_list[inx]
            # Expected to be option:
            if not inx % 2: # Odd arguments (with even index!)

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
                last_option = arg_list[inx-1]
                if option.startswith('-'):
                    return {"error": f"The '{option}' is not a valid value for '{last_option}'"}
                arg_map.update({last_key: option})

        # Verify the options and their arguments
        for key, value in arg_map.items():
            # Make sure all the items have a value:
            if not value:
                return {"error": f"The '{key}' option has no value"}

            # verify the selected 'status' if -js was selected
            if key == 'js':
                for status, opts in valid_status.items():
                    # Replace the js value with the right criteria
                    if value in opts:
                        arg_map.update({'js': status})
                        break
                else:
                    return {"error": f"The '{value}' is not valid argument for '{key}' option."}

            # Number of days must be >1
            if key == 'days':
                if int(value) < 1:
                    return {"error": f"The Number of days cannot be less than '1'"}

        # For finished jobs the number of days ago must be selected
        if arg_map.get('js', None) == "FINISHED" and not arg_map.get('days', None):
            return {"error": f"For 'FINISHED' jobs, [-d | --days] option must be defined"}

        # When JobId is defined, the cluster has to be defined too
        if arg_map.get('jobid', None) and not arg_map.get('cluster', None):
            return {"error": f"The cluster name must be defined by [-c | --cluster] one a 'jobid' is selected"}

        # No TaskId can be defined without their JobId
        if arg_map.get('taskid', None) and not arg_map.get('jobid', None):
            return {"error": f"jobid must be selected by [-j | --jobid] for the given 'taskid'"}

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
            prompt += f" [{Color.BLUE}jobs{Color.NO_COLOR}]"
        elif self.session.mode == self.Mode.SERVER:
            prompt += f" [{Color.GREEN}{self.session.serverName}{Color.NO_COLOR}]"
        elif self.session.mode == self.Mode.TARGET:
            prompt += f" [{Color.GREEN}{self.session.serverName}{Color.NO_COLOR}]->"
            prompt += f"[{Color.RED_L}{self.session.targetName}{Color.NO_COLOR}]"
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


class Color:
    """
        ANSI codes for terminal colors
    """
    NO_COLOR = "\033[0m"
    RED = "\033[0;31m"
    RED_L = "\033[1;31m"
    GREEN = "\033[0;32m"
    BLUE = "\033[1;34m"
    CYAN = "\033[0;36m"
    YELLOW = "\033[1;33m"
    ORANGE = "\033[0;33m"
    PURPLE = "\033[0;35m"
    GREEN_L = "\033[1;32m"



if __name__ == '__main__':
    try:
        ProvenanceShell().cmdloop()
    except KeyboardInterrupt:
        print("Bye!")