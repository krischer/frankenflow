import copy
import os
import shutil

from celery.result import AsyncResult

from . import (celery_tasks, config, flow_graph, flow_status, utils, tasks,
               NoJobsLeft, push_notifications)


class FlowManager():
    def __init__(self, base_folder):
        os.makedirs(base_folder, exist_ok=True)
        self.base_folder = base_folder

        # Init status and config.
        self.status = flow_status.FlowStatus(os.path.join(self.base_folder,
                                                          "status.json"))
        self.config = config.Config(os.path.join(self.base_folder,
                                                 "config.json"))

        # Working directory for all the jobs.
        self.working_dir = os.path.join(self.base_folder, "__JOBS")
        os.makedirs(self.working_dir, exist_ok=True)

        # Directory where the optimization takes place.
        self.optimization_dir = os.path.join(self.base_folder,
                                             "__OPTIMIZATION")
        os.makedirs(self.optimization_dir, exist_ok=True)

        self.seismopt_dir = os.path.join(self.base_folder, "seismopt")

        # Folders to collect the output.
        output_folder = os.path.join(self.base_folder, "__OUTPUT")

        self.output_folders = {
            "regular_grid_models": os.path.join(output_folder,
                                                "regular_grid_models"),
            "spec_elem_grid_models": os.path.join(output_folder,
                                                  "spec_elem_grid_models"),
            "regular_grid_gradients": os.path.join(output_folder,
                                                   "regular_grid_gradients"),
            "unsmoothed_spectral_element_grid_gradients": os.path.join(
                output_folder, "unsmoothed_spectral_element_grid_gradients"),
            # Collect all the misfits in simple text files.
            "misfits": os.path.join(output_folder, "misfits"),
            # Store all the seismopt next files.
            "seismopt_next_files": os.path.join(output_folder,
                                                "seismopt_next_files")
        }

        for folder in self.output_folders.values():
            os.makedirs(folder, exist_ok=True)

        self.__check_data_files()

        self.graph = flow_graph.FlowGraph(
            filename=os.path.join(self.base_folder, "graph.json"))

    @property
    def current_status(self):
        return {
            "status": self.status["current_status"],
            "message": self.status["current_message"],
            "goal": self.status["current_goal"]
        }

    def reset_job(self, job_id):
        # Get the job.
        job = copy.deepcopy(self.graph[job_id])

        try:
            del job["celery_task_id"]
        except KeyError:
            pass

        try:
            del job["run_information"]
        except KeyError:
            pass

        # Reset files.
        files = ["stdout", "stderr", "logfile"]
        for filename in files:
            if filename not in job:
                continue

            filename = job[filename]

            if not os.path.exists(filename):
                continue

            os.remove(filename)

        if "working_dir" in job:
            wd = job["working_dir"]
            if os.path.exists(wd):
                shutil.rmtree(wd)
            os.makedirs(wd, exist_ok=True)

        job["job_status"] = "not started"

        self.graph[job_id] = job
        self.graph.serialize()

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

            push_notifications.send_notification(
                title="Workflow encountered error.",
                message="Problem with main iterate function: %s" % tb)

    def _iterate(self):
        # If there is no job, create one!
        if len(self.graph) == 0:
            self.create_initial_job()


        assert len(self.graph) != 0, "The graph must not be empty!"

        try:
            job_id = self.graph.get_current_or_next_job()
        except NoJobsLeft:
            self.status["current_status"] = "DONE"
            self.status["current_message"] = "No jobs left"

            push_notifications.send_notification(
                title="Workflow done.",
                message="No more jobs left")

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

                            if "inputs" in step:
                                inputs = copy.deepcopy(step["inputs"])
                            else:
                                inputs = {}

                            # Pass along previous inputs for everything but
                            # the orchestrate task. That task can reset the
                            # inputs and only pass along the required inputs.
                            if job["task_type"] != "Orchestrate":
                                inputs.update(job["inputs"])

                            prio = step["priority"] \
                                if "priority" in step else 0
                            self.graph.add_job(
                                task_type=step["task_type"],
                                inputs=inputs,
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
                    push_notifications.send_notification(
                        title="Workflow encountered error.",
                        message="Job exited with status 'failed'.")
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
            inputs={"regular_model_folder": folder},
            from_node=job_id
        )

        # Add a job to plot the starting model at a higher priority.
        self.graph.add_job(
            task_type="PlotRegularGridModel",
            inputs={"regular_model_folder": folder},
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
            "block_z": "SES3D block_z file",
            "seismopt/opt_settings.xml": "Initial seismopt settings",
            "seismopt/optlib.exe": "seismopt executable",
            "seismopt/ses3d.cfg": "seismopt configuration"
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
            "optimization_dir": self.optimization_dir,
            "seismopt_dir": self.seismopt_dir,
            "data": self.data,
            "config": self.config.config,
            "output_folders": self.output_folders
        }