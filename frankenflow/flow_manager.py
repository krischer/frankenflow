import copy
import datetime
import json
import os
import uuid

from celery.result import AsyncResult
import networkx as nx
import networkx.readwrite.json_graph

from . import celery_tasks, utils, tasks


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
        self._assert_config_file_exists("lasif_cmd")

        self._assert_config_folder_exists("lasif_project")

        # Variables to deal with the HPC remote host.
        self._assert_var_exists("hpc_remote_host")
        self._assert_var_exists("hpc_agere_project")
        self._assert_var_exists("hpc_remote_input_files_directory")

        self._assert_var_exists("number_of_events", var_type=int)
        self._assert_var_exists("forward_wavefield_storage_degree",
                                var_type=int)
        self._assert_var_exists("parallel_events", var_type=int)
        self._assert_var_exists("pml_count", var_type=int)
        self._assert_var_exists("walltime_per_event_forward", var_type=float)
        self._assert_var_exists("walltime_per_event_adjoint", var_type=float)


    def _assert_var_exists(self, key, var_type=None):
        assert key in self.config, \
            "'%s' must be given in the config file." % key
        if var_type is not None:
            assert isinstance(self.config[key], var_type), \
                "Config variable '%s' not of type '%s'." % (key,
                                                            var_type.__name__)

    def _assert_config_file_exists(self, key):
        self._assert_var_exists(key)

        filename = self.config[key]
        assert os.path.isfile(filename), \
            "File '%s' for config value '%s' must exist." % (filename, key)

    def _assert_config_folder_exists(self, key):
        self._assert_var_exists(key)

        filename = self.config[key]
        assert os.path.isdir(filename), \
            "Folder '%s' for config value '%s' must exist." % (filename, key)

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
        if os.path.exists(self.filename):
            self.deserialize()
        else:
            self.graph = nx.DiGraph()

    def serialize(self):
        with open(self.filename, "wt") as fh:
            json.dump(self.get_json(), fh)

    def deserialize(self):
        with open(self.filename, "rt") as fh:
            self.graph = networkx.readwrite.json_graph.node_link_graph(
                json.load(fh))

    def get_json(self):
        return networkx.readwrite.json_graph.node_link_data(self.graph)

    def add_job(self, task_type, inputs, priority=0, from_node=None):
        now = datetime.datetime.now()
        graph_id = now.strftime("%y%m%dT%H%M%S_") + task_type + "_" + str(
            uuid.uuid4())
        self.graph.add_node(n=graph_id, attr_dict={
            "task_type": task_type,
            "inputs": inputs,
            "priority": priority,
            "job_status": "not started"
        })

        if from_node:
            print("Adding edge from", from_node, "to", graph_id)
            self.graph.add_edge(from_node, graph_id)

        return graph_id, self[graph_id]

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
                           key=lambda x: self.graph.node[x]["priority"],
                           reverse=True)

        if not out_nodes:
            raise NoJobsLeft

        return out_nodes[0]

    def __getitem__(self, item):
        return self.graph.node[item]

    def __len__(self):
        return len(self.graph)


class FlowStatus(object):
    """
    Simple persistent status.

    Like a dictionary just always stored on disc.
    """
    def __init__(self, filename):
        self._filename = filename
        self._deserialize()

    def __getitem__(self, item):
        if item not in self.__status:
            return None
        self._deserialize()
        return self.__status[item]

    def __setitem__(self, key, value):
        self.__status[key] = value
        self._serialize()

    def _deserialize(self):
        if not os.path.exists(self._filename):
            self.__status = {}
        else:
            with open(self._filename, "rt") as fh:
                self.__status = json.load(fh)
        return self.__status

    def _serialize(self):
        with open(self._filename, "wt") as fh:
            json.dump(self.__status, fh)


class FlowManager():
    def __init__(self, base_folder):
        os.makedirs(base_folder, exist_ok=True)
        self.base_folder = base_folder

        # Init status and config.
        self.status = FlowStatus(os.path.join(self.base_folder, "status.json"))
        self.config = Config(os.path.join(self.base_folder,
                                          "config.json"))

        # Working directory for all the jobs.
        self.working_dir = os.path.join(self.base_folder, "__JOBS")
        os.makedirs(self.working_dir, exist_ok=True)

        # Directory where the optimization takes place.
        self.optimization_dir = os.path.join(self.base_folder,
                                             "__OPTIMIZATION")
        os.makedirs(self.optimization_dir, exist_ok=True)

        # Folders to collect the output.
        output_folder = os.path.join(self.base_folder, "__OUTPUT")

        self.output_folders = {
            "regular_grid_models": os.path.join(output_folder,
                                                "regular_grid_models"),
            "spec_elem_grid_models": os.path.join(output_folder,
                                                  "spec_elem_grid_models"),
            "regular_grid_kernels": os.path.join(output_folder,
                                                 "regular_grid_kernels"),
            # Collect all the misfits in simple text files.
            "misfits": os.path.join(output_folder, "misfits")
        }

        for folder in self.output_folders.values():
            os.makedirs(folder, exist_ok=True)

        self.__check_data_files()

        self.graph = FlowGraph(
            filename=os.path.join(self.base_folder, "graph.json"))

    @property
    def current_status(self):
        return {
            "status": self.status["current_status"],
            "message": self.status["current_message"],
            "goal": self.status["current_goal"]
        }

    def iterate(self):
        """
        Attempt to advance the workflow a single step.

        Should be periodically called by something.
        """
        try:
            self._iterate()
        except Exception:
            tb = utils.collect_traceback(3)
            self.status["current_message"] = tb
            self.status["current_status"] = "ERROR"

    def _iterate(self):
        # If there is no job, create one!
        if len(self.graph) == 0:
            self.create_initial_job()


        assert self.status["current_goal"] is not None, \
            "Cannot advance the flow without a goal!"

        assert len(self.graph) != 0, "The graph has an id thus it must not " \
                                     "be empty!"

        try:
            job_id = self.graph.get_current_or_next_job()
        except NoJobsLeft:
            self.status["current_status"] = "DONE"
            self.status["current_message"] = "No jobs left"
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
                self.status["current_status"] = "OK"
                self.status["current_message"] = \
                    "Job '%s' currently running." % job_id
            # Run finished. A run should not really fail.
            elif result.state == "SUCCESS":
                return_value = result.wait()
                job["run_information"] = return_value
                if return_value["status"] == "success":
                    job["job_status"] = "success"
                    self.status["current_message"] = \
                        "Successfully completed job '%s'." % (job_id)
                    self.status["current_status"] = "SUCCESS"

                    if return_value["next_steps"]:
                        for step in return_value["next_steps"]:
                            prio = step["priority"] \
                                if "priority" in step else 0
                            self.graph.add_job(
                                task_type=step["task_type"],
                                inputs=step["inputs"],
                                priority=prio,
                                from_node=job_id)

                    if "new_goal" in return_value:
                        self.status["current_goal"] = return_value["new_goal"]

                elif return_value["status"] == "failed":
                    job["job_status"] = "failed"
                    self.status["current_status"] = "Failure"
                    fs = return_value["fail_stage"]
                    self.status["current_message"] = \
                        "Job '%s' failed at stage '%s' due to: '%s'" % (
                            job_id, fs, return_value[fs]["fail_reason"])
                else:
                    job["job_status"] = return_value["status"]
                    self.status["current_status"] = "????"
                    self.status["current_message"] = \
                        "Current status is not clear. Celery job returned " \
                        "with status '%s'." % return_value["status"]

                # No matter the outcome. Always save the graph.
                self.graph.serialize()
            else:
                job["job_status"] = result.state
                self.status["current_status"] = "????"
                self.status["current_message"] = \
                    "Current status is not clear."
        elif job["job_status"] == "not started":
            self.start_job(job_id)
            self.status["current_status"] = "OK"
            self.status["current_message"] = "Job '%s' started." % job_id
        elif job["job_status"] == "failed":
            # Nothing to do. Requires to restart the latest job!
            return
        else:
            raise NotImplementedError("'job_status' = '%s'" %
                                      job["job_status"])

    def start_job(self, job_id):
        job_information = copy.deepcopy(self.graph[job_id])
        job_information["job_id"] = job_id

        job_information["working_dir"] = os.path.join(
            self.info["working_dir"], job_id)
        job_information["stdout"] = os.path.join(
            job_information["working_dir"], "stdout")
        job_information["stderr"] = os.path.join(
            job_information["working_dir"], "stderr")
        job_information["logfile"] = os.path.join(
            job_information["working_dir"], "logfile.txt")

        job_class = tasks.task_map[job_information["task_type"]]

        # Pass on the goal if the task at hand requires it.
        if job_class.task_requires_active_goal:
            job_information["current_goal"] = self.current_status["goal"]
            self.graph[job_id]["current_goal"] = self.current_status["goal"]

        # The orchestrate node is special. It does require information about
        # the current goal to be able to deduce the next.
        if job_information["task_type"] == "Orchestrate":
            job_information["inputs"]["current_goal"] = \
                self.current_status["goal"]

        result = celery_tasks.launch_job.delay(job_information,
                                               context=self.info)
        self.graph[job_id]["job_status"] = "running"
        self.graph[job_id]["celery_task_id"] = result.task_id

        keys = ["working_dir", "stdout", "stderr", "logfile"]
        for key in keys:
            self.graph[job_id][key] = job_information[key]

        self.graph.serialize()

    def create_initial_job(self):
        """
        Create the first job which is then used to trigger the rest of the
        workflow. Each step will always triggers its next step.
        """
        # Must have an initial model. The rest will be derived.
        folder = os.path.join(self.optimization_dir, "000_1_model")
        assert os.path.exists(folder) and os.path.isdir(folder), (
            "Could not create the initial jobs. No folder '%s' found." %
            folder)

        # Add a start node that does not do anything.
        job_id, start = self.graph.add_job(task_type="Start", inputs={})
        start["job_status"] = "success"

        # Add a project model task at the default priority.
        self.graph.add_job(
            task_type="ProjectModel",
            inputs={"model_folder": folder},
            from_node=job_id
        )

        # Add a job to plot the starting model at a higher priority.
        self.graph.add_job(
            task_type="PlotRegularGridModel",
            inputs={"model_folder": folder},
            priority=1,
            from_node=job_id
        )

        self.graph.serialize()

        # Set the current goal to calculate the misfit for model 000_1_model
        self.status["current_goal"] = "misfit 000_1_model"

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
            "data_folder": self.data_folder,
            "working_dir": self.working_dir,
            "data": self.data,
            "config": self.config.config,
            "output_folders": self.output_folders
        }