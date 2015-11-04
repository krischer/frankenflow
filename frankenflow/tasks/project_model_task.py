import os
import shutil

from . import task
from .. import utils


class ProjectModelTask(task.Task):
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

        self._output_directory = os.path.join(self.working_dir,
                                              "projected_model")
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
        self._run_external_script(cwd=".", cmd=cmd)

    def check_post_run(self):
        pass

    def generate_next_steps(self):
        pass
