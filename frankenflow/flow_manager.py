import copy
import datetime
import glob
import json
import os
import uuid

from celery.result import AsyncResult
import networkx as nx
import networkx.readwrite.json_graph

from . import celery_tasks, utils


class NoJobsLeft(Exception):
    pass


class Config():
    def __init__(self, filename):
        self.__filename = filename

        if not os.path.exists(filename):
            self.config = {
            }
            self.serialize()
        else:
            self.deserialize()

        self.assert_config()

    def assert_config(self):
        self._assert_config_file_exists("agere_cmd")

    def _assert_config_file_exists(self, key):
        assert key in self.config, \
            "'%s' must be given in the config file." % key

        filename = self.config[key]
        assert os.path.isfile(filename), \
            "File '%s' for config value '%s' must exist." % (filename, key)

    def serialize(self):
        with open(self.__filename, "wt") as fh:
            json.dump(self.config, fh)

    def deserialize(self):
        with open(self.__filename, "rt") as fh:
            self.config = json.load(fh)
        self.assert_config()

    def __getitem__(self, item):
        return self.config[item]


class FlowGraph():
    def __init__(self, filename):
        self.filename = filename
        self.graph = nx.DiGraph()

    def add_job(self, task_type, inputs):
        now = datetime.datetime.now()
        graph_id = now.strftime("%y%m%dT%H%M%S_") + str(uuid.uuid4())
        self.graph.add_node(n=graph_id, attr_dict={
            "task_type": task_type,
            "inputs": inputs,
            "priority": 0,
            "job_status": "not started"
        })

    def get_current_or_next_job(self):
        """
        Get the current or next job.
        """
        # Find all jobs that have no outwards pointing edges.
        out_nodes = [i for i in self.graph.nodes_iter() if
                     self.graph.out_degree(i) == 0]
        if not out_nodes:
            raise NoJobsLeft

        # Find the one that has status == running
        running_nodes = [i for i in out_nodes
                         if self.graph.node[i]["job_status"] == "running"]
        assert len(running_nodes) <= 1, "Only one job can be active at any " \
                                        "given time."
        # One running node.
        if running_nodes:
            return running_nodes[0]

        # Make sure non of the out_nodes has a success state.
        out_nodes = [i for i in out_nodes
                     if self.graph.node[i]["job_status"] != "success"]

        # Now pick the job with the highest priority
        out_nodes = sorted(out_nodes,
                           key=lambda x: self.graph.node[x]["priority"])

        if not out_nodes:
            raise NoJobsLeft

        return out_nodes[0]

    def __getitem__(self, item):
        return self.graph.node[item]

    def get_json(self):
        return networkx.readwrite.json_graph.node_link_data(self.graph)

    def __len__(self):
        return len(self.graph)


class FlowManager():
    def __init__(self, base_folder):
        os.makedirs(base_folder, exist_ok=True)
        self.base_folder = base_folder

        self.config = Config(os.path.join(self.base_folder,
                                               "config.json"))

        self.working_dir = os.path.join(self.base_folder, "__JOBS")
        os.makedirs(self.working_dir, exist_ok=True)

        self.__check_data_files()

        self.graph = FlowGraph(
            filename=os.path.join(self.base_folder, "graph.json"))

        self._current_status = None
        self._current_message = None

    @property
    def current_status(self):
        return {
            "status": self._current_status,
            "message": self._current_message
        }

    def iterate(self):
        """
        Attempt to advance the workflow a single step.

        Should be periodically called by something.
        """
        self._current_status = None
        self._current_message = None

        try:
            self._iterate()
        except Exception:
            tb = utils.collect_traceback(3)
            self._current_message = tb
            self._current_status = "ERROR"

    def _iterate(self):
        # If there is no job, create one!
        if len(self.graph) == 0:
            self.create_initial_job()

        assert len(self.graph) != 0, "The graph has an id thus it must not " \
                                     "be empty!"

        try:
            job_id = self.graph.get_current_or_next_job()
        except NoJobsLeft:
            self._current_status = "DONE"
            self._current_message = "No jobs left"
            return
        self.advance_job(job_id)

    def advance_job(self, job_id):
        job = self.graph[job_id]

        if job["job_status"] == "running":
            # Check if it is still running.
            result = AsyncResult(id=job["celery_task_id"])
            print("STATE:", result.state)
            # Still running. Nothing to be done.
            if result.state == "SENT":
                self._current_status = "OK"
                self._current_message = "Job '%s' currently running." % job_id
            # Run finished. A run should not really fail.
            elif result.state == "SUCCESS":
                return_value = result.wait()
                job["run_information"] = return_value
                if return_value["status"] == "success":
                    job["job_status"] = "success"
                    self.create_next_job(job_id)
                    self._current_message = (
                        "Successfully completed job '%s'." % (job_id))
                    self._current_status = "SUCCESS"
                elif return_value["status"] == "failed":
                    job["job_status"] = "failed"
                    self._current_status = "Failure"
                    fs = return_value["fail_stage"]
                    self._current_message = (
                        "Job '%s' failed at stage '%s' due to: '%s'" % (
                            job_id, fs, return_value[fs]["fail_reason"]))
                else:
                    job["job_status"] = return_value["status"]
                    self._current_status = "????"
                    self._current_message = \
                        "Current status is not clear. Celery job returned " \
                        "with status '%s'." % return_value["status"]
            else:
                job["job_status"] = result.state
                self._current_status = "????"
                self._current_message = "Current status is not clear."
        elif job["job_status"] == "not started":
            self.start_job(job_id)
            self._current_status = "OK"
            self._current_message = "Job '%s' started." % job_id
        elif job["job_status"] == "failed":
            # Nothing to do. Requires to restart the latest job!
            return
        else:
            raise NotImplementedError("'job_status' = '%s'" %
                                      job["job_status"])

    def create_next_job(self, job_id):
        print("Creating next job!!!!")

    def start_job(self, job_id):
        job_information = copy.deepcopy(self.graph[job_id])
        job_information["job_id"] = job_id

        job_information["working_dir"] = os.path.join(
            self.info["working_dir"], job_id)
        job_information["stdout"] = os.path.join(
            job_information["working_dir"], "stdout")
        job_information["stderr"] = os.path.join(
            job_information["working_dir"], "stderr")

        result = celery_tasks.launch_job.delay(job_information,
                                               context=self.info)
        self.graph[job_id]["job_status"] = "running"
        self.graph[job_id]["celery_task_id"] = result.task_id

        keys = ["working_dir", "stdout", "stderr"]
        for key in keys:
            self.graph[job_id][key] = job_information[key]

    def create_initial_job(self):
        """
        Create the first job which is then used to trigger the rest of the
        workflow. Each step will always triggers its next step.
        """
        # Find all folders in the base_directory with 'XXX_1_Model'. This is
        # where everything starts.
        folders = glob.glob(os.path.join(self.base_folder, "???_1_Model"))
        folders = [_i for _i in folders if os.path.isdir(_i)]
        if not folders:
            raise ValueError("Could not create initial job. No folder with "
                             "pattern `???_1_Model` found.")
        # Now the folder is found.
        folder = sorted(folders)[0]

        self.graph.add_job(
            task_type="ProjectModelTask",
            inputs={"model_folder": folder}
        )

    def __check_data_files(self):
        # A couple of files are needed.
        self.data_folder = os.path.join(self.base_folder, "__DATA")
        os.makedirs(self.data_folder, exist_ok=True)

        required_files = {
            "input_files/setup": "SES3D INPUT/setup files",
            "block_x": "SES3D block_x file",
            "block_y": "SES3D block_y file",
            "block_z": "SES3D block_z file"
        }

        for filename, description in required_files.items():
            filename = os.path.join(os.path.abspath(os.path.join(
                self.data_folder, filename)))
            if not os.path.exists(filename):
                raise ValueError(
                    "File '%s' must exists before we can start. Description "
                    "of the file: %s" % (filename, description))

        self.data = {}
        self.data["input_folder"] = os.path.join(self.data_folder,
                                                 "input_files")
        self.data["block_files"] = [os.path.join(self.data_folder, _i) for
                                    _i in ["block_x", "block_y", "block_z"]]

    @property
    def info(self):
        return {
            "base_folder": self.base_folder,
            "working_dir": self.working_dir,
            "data": self.data,
            "config": self.config.config
        }