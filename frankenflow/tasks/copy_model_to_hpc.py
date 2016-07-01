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
        return {"iteration_name"}

    def check_pre_staging(self):
        raise NotImplementedError
        # Make sure the folder does exist.
        assert os.path.exists(self.binary_model_path), \
            "'%s' does not exist" % self.binary_model_path
        assert os.path.isdir(self.binary_model_path), \
            "'%s' is not a folder" % self.binary_model_path

        self._init_ssh_and_stfp_clients()

        # Also make sure it does not yet exist on the HPC side. Directly
        # copy it to the HPC agere's __MODEL directory.
        self.remote_model_directory = os.path.join(
            self.context["config"]["hpc_agere_project"],
            "__MODELS")

        # Make sure this directory exists but does not have the model yet.
        if "__MODELS" not in self.remote_listdir(
                self.context["config"]["hpc_agere_project"]):
            self.remote_mkdir(self.remote_model_directory)

        existing_models = self.remote_listdir(self.remote_model_directory)
        assert self.model_name not in existing_models, (
            "Model '%s' already exists in %s:%s" % (
                self.model_name,
                self.context["config"]["hpc_remote_host"],
                self.remote_model_directory))

        self.remote_target_directory = os.path.join(
            self.remote_model_directory, self.model_name)

    def stage_data(self):
        self.remote_mkdir(self.remote_target_directory)

    def check_post_staging(self):
        existing_models = self.remote_listdir(self.remote_model_directory)
        assert self.model_name in existing_models, (
            "Creating folder %s:%s failed." % (
                self.context["config"]["hpc_remote_host"],
                self.remote_target_directory))

    def run(self):
        for filename in os.listdir(self.binary_model_path):
            src = os.path.join(self.binary_model_path, filename)
            target = os.path.join(self.remote_target_directory, filename)
            self.remote_put(src, target)

    def check_post_run(self):
        # Make sure all files have been copied.
        remote_files = set(
            self.remote_listdir(self.remote_target_directory))

        local_files = set(os.listdir(self.binary_model_path))

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
