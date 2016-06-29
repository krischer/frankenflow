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
        raise Exception
        # Make sure the kernels have been copied.
        kernel_folder = os.listdir(
            self.inputs["local_binary_gradient_directory"])
        assert len(kernel_folder) > 4, "Binary gradient files to not exist."

        self.local_hdf5_gradient_filename = \
            os.path.join(self.working_dir, "gradient_%s.h5" %
                         self.inputs["iteration_name"])

        assert not os.path.exists(self.local_hdf5_gradient_filename), \
            "'%s' does already exist" % self.local_hdf5_gradient_filename

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        cmd = [
            self.context["config"]["agere_cmd"],
            "hdf5_model_to_binary",
            self.hdf5_model_path,
            self.binary_model_path]
        self._run_external_script(cwd=".", cmd=cmd)

    def check_post_run(self):
        # Now it should exist.
        assert os.path.exists(self.binary_model_path), \
            "'%s' did not get created." % self.binary_model_path

    def generate_next_steps(self):
        next_steps = [
            # Produce a plot of the projected model.
            {"task_type": "PlotSES3DBinaryFormatModel",
             "priority": 1
             },
            # Copy the model to the HPC.
            {"task_type": "CopyModelToHPC",
             "priority": 0
             }
        ]
        return next_steps
