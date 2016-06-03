import os

from . import task


class CopyModelToHPC(task.Task):
    """
    Task copying a model on the spectral element grid to the HPC running the
    simulation.

    The model must reside in the associated LASIF project.
    """
    @property
    def required_inputs(self):
        return {"model_name"}

    def check_pre_staging(self):
        self.inputs["model_name"] = self.inputs["model_name"].lower()

        # Make sure the folder does exist.
        self.model_folder = os.path.join(
            self.context["config"]["lasif_project"], "MODELS",
            self.inputs["model_name"])
        assert os.path.exists(self.model_folder), \
            "'%s' does not exist" % self.model_folder
        assert os.path.isdir(self.model_folder), \
            "'%s' is not a folder" % self.model_folder

        self._init_ssh_and_stfp_clients()

        # Also make sure it does not yet exist on the HPC side. Directly
        # copy it to the HPC agere's __MODEL directory.
        self.remote_model_directory = os.path.join(
            self.context["config"]["hpc_agere_project"],
            "__MODELS")

        print("================")
        print(self.remote_model_directory)
        print("================")

        # Make sure this directory exists but does not have the model yet.
        existing_models = self.remote_listdir(self.remote_model_directory)
        assert self.inputs["model_name"] not in existing_models, (
            "Model '%s' already exists in %s:%s" % (
                self.inputs["model_name"],
                self.context["config"]["hpc_remote_host"],
                self.remote_model_directory))

        self.remote_target_directory = os.path.join(
            self.remote_model_directory, self.inputs["model_name"])

        self.remote_mkdir(self.remote_target_directory)

        # Make sure this worked.
        existing_models = self.remote_listdir(self.remote_model_directory)
        assert self.inputs["model_name"] in existing_models, (
            "Creating folder %s:%s failed." % (
                self.context["config"]["hpc_remote_host"],
                self.remote_target_directory))

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        self.input_files = os.listdir(self.model_folder)
        for filename in self.input_files:
            src = os.path.join(self.model_folder, filename)
            target = os.path.join(self.remote_target_directory, filename)
            self.remote_put(src, target)

    def check_post_run(self):
        # Make sure all files have been copied.
        remote_files = set(
            self.remote_listdir(self.remote_target_directory))

        local_files = set(self.input_files)

        missing_files = local_files.difference(remote_files)

        assert not missing_files, \
            "The following files could not be copied: %s" % (
                ", ".join(missing_files))

    def generate_next_steps(self):
        next_steps = [
            # Run the forward adjoint.
            {"task_type": "ForwardSimulation",
             "priority": 0
            }
        ]
        return next_steps
