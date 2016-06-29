import os

from . import task


class ConvertModelToBinary(task.Task):
    """
    Convert an HDF5 model to the binary format required by SES3D.
    """
    @property
    def required_inputs(self):
        return {"iteration_name"}

    def check_pre_staging(self):
        filename = self.hdf5_model_path
        assert os.path.exists(filename), "'%s' does not exist" % filename

        # Make sure it does not yet exist.
        assert not os.path.exists(self.binary_model_path), \
            "'%s' does already exist" % self.binary_model_path

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
