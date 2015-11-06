import os
import shutil

from . import task
from .. import utils


class ProjectModel(task.Task):
    """
    Task projecting a model.
    """
    def check_pre_staging(self):
        # Make sure all required input files exist.
        assert "model_folder" in self.inputs
        utils.assert_files_exist(
            self.inputs["model_folder"], ["x_rho", "x_vp", "x_vsh", "x_vsv"])

        # Make sure the output files don't.
        utils.assert_files_dont_exist(self.working_dir, [
           "drho", "dvp", "dvsh", "dvsv", "block_x", "block_y", "block_z"])

        # Copy directly to LASIF directory.
        self._output_directory = os.path.join(
            self.context["config"]["lasif_project"], "MODELS",
            os.path.basename(self.inputs["model_folder"]))

        assert not os.path.exists(self._output_directory), \
            "Folder '%s' already exists." % self._output_directory

    def stage_data(self):
        self.copy_blockfiles(target_dir=self.working_dir)
        # Copy data and rename it to what the Fortran program expects.
        os.makedirs(self.working_dir, exist_ok=True)
        cp_map = {
            "x_rho": "drho",
            "x_vp": "dvp",
            "x_vsh": "dvsh",
            "x_vsv": "dvsv"
        }
        for src, dest in cp_map.items():
            src = os.path.join(self.inputs["model_folder"], src)
            dest = os.path.join(self.working_dir, dest)
            shutil.copy2(src, dest)

    def check_post_staging(self):
        utils.assert_files_exist(
            self.working_dir,
            ["drho", "dvp", "dvsh", "dvsv", "block_x", "block_y", "block_z"])

    def run(self):
        cmd = [self.context["config"]["agere_cmd"],
               "model_to_spectral_element_grid",
               "--output-folder=%s" % self._output_directory,
               "--input-files=%s" % self.context["data"]["input_folder"],
               self.working_dir]
        returncode = self._run_external_script(cwd=".", cmd=cmd)
        assert returncode == 0, \
            "Model projection exited with return code %i." % returncode

    def check_post_run(self):
        # The output directory must have a couple of files now. This gives a
        # certain confidence that the operation succeeded
        utils.assert_files_exist(
            self._output_directory, ["A0", "B0", "C0", "boxfile", "lambda0",
                                     "rhoinv0", "mu0"])

    def generate_next_steps(self):
        next_steps = [
            # Produce a plot of the projected model.
            {"task_type": "PlotSpectralElementGridModel",
             "inputs": {
                 "model_name": os.path.basename(self._output_directory)
             },
             "priority": 1
            },
            # Copy the model to the HPC.
            {"task_type": "CopyModelToHPC",
             "inputs": {
                 "model_name": os.path.basename(self._output_directory)
             },
             "priority": 0
             }
        ]
        return next_steps
