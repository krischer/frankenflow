import os

from . import task


class ConvertGradientsToHDF5(task.Task):
    """
    Convert binary kernels to HDF5.
    """
    @property
    def required_inputs(self):
        return {"local_binary_gradient_directory", "iteration_name"}

    def check_pre_staging(self):
        # Make sure the kernels have been copied.
        kernel_folder = os.listdir(
            self.inputs["local_binary_gradient_directory"])
        assert len(kernel_folder) > 4, "Binary gradient files to not exist."

        self.hdf5_gradient_filename = \
            self.get_gradient_file(self.inputs["iteration_name"])

        assert not os.path.exists(self.hdf5_gradient_filename), \
            "'%s' does already exist" % self.hdf5_gradient_filename

    def stage_data(self):
        # The gradient needs a boxfile. Just get it from the corresponding
        # model which should always have one.
        self.boxfile = os.path.join(
            self.inputs["local_binary_gradient_directory"], "boxfile")
        if os.path.exists(self.boxfile):
            return
        cmd = [
            "h5dump",
            "-d",
            "_meta/boxfile",
            "-b",
            "-o",
            self.boxfile,
            self.hdf5_model_path]
        self._run_external_script(cwd=".", cmd=cmd)

    def check_post_staging(self):
        assert os.path.exists(self.boxfile), \
            "boxfile could not be extracted from the model for some reason."

    def run(self):
        cmd = [
            self.context["config"]["agere_cmd"],
            "binary_model_to_hdf5",
            self.c["lasif_project"],
            self.inputs["local_binary_gradient_directory"],
            self.hdf5_gradient_filename]
        self._run_external_script(cwd=".", cmd=cmd)

    def check_post_run(self):
        # Now it should exist.
        assert os.path.exists(self.hdf5_gradient_filename), \
            "'%s' did not get created." % self.hdf5_gradient_filename

    def generate_next_steps(self):
        next_steps = [
            # Plot the raw gradient.
            {"task_type": "PlotHDF5Gradient",
             "inputs": {"tag": ""},
             "priority": 1
             },
            {"task_type": "TaperAndPreconditionGradient",
             "priority": 1
             }
        ]
        return next_steps
