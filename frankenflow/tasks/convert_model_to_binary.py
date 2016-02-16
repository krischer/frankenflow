import os

from . import task


class ConvertModelToBinary(task.Task):
    """
    Convert an HDF5 model to the binary format required by SES3D.
    """
    @property
    def required_inputs(self):
        return {"hdf5_model_path"}

    def check_pre_staging(self):
        filename = self.inputs["hdf5_model_path"]
        assert os.path.exists(filename), "'%s' does not exist" % filenmae

        # The file will be created in the LASIF project.
        model_name = os.path.splitext(os.path.basename(filename))[0]
        self.model_folder = os.path.join(
                self.context["config"]["lasif_project"], "MODELS",
                model_name)
        # Make sure it does not yet exist.
        assert not os.path.exists(self.model_folder), \
            "'%s' does already exist" % self.model_folder

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        cmd = [
            self.context["config"]["agere_cmd"],
            "hdf5_model_to_binary",
            self.inputs["hdf5_model_path"],
            self.model_folder]
        self._run_external_script(cwd=".", cmd=cmd)

    def check_post_run(self):
        # Now it should exist.
        assert os.path.exists(self.model_folder), \
            "'%s' did not get created." % self.model_folder

    def generate_next_steps(self):
        next_steps = [
            # Produce a plot of the projected model.
            {"task_type": "PlotSpectralElementGridModel",
             "inputs": {
                 "model_name": os.path.basename(self.model_folder)
             },
             "priority": 1
             },
            # Copy the model to the HPC.
            {"task_type": "CopyModelToHPC",
             "inputs": {
                 "model_name": os.path.basename(self.model_folder)
             },
             "priority": 0
             }
        ]
        return next_steps
