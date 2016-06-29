import os
import shutil

from . import task
from .. import utils


class PlotRegularGridModel(task.Task):
    """
    Task plotting a regular grid model.
    """
    # No goal required for plotting. It is just a side activity.
    task_requires_active_goal = False

    @property
    def required_inputs(self):
        return {"regular_model_folder"}

    def check_pre_staging(self):
        utils.assert_files_exist(
            self.inputs["regular_model_folder"],
            ["x_rho", "x_vp", "x_vsh", "x_vsv"])

        self.model_name = os.path.basename(self.inputs["regular_model_folder"])

        # Make sure the output files don't.
        self.outputs = {
            "x_rho": self.model_name + "_rho_100km_depth.jpg",
            "x_vp": self.model_name + "_vp_100km_depth.jpg",
            "x_vsh": self.model_name + "_vsh_100km_depth.jpg",
            "x_vsv": self.model_name + "_vsv_100km_depth.jpg",
        }

        utils.assert_files_dont_exist(self.working_dir,
                                      list(self.outputs.keys()))

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        return
        for key, value in self.outputs.items():
            cmd = [
                self.context["config"]["agere_cmd"],
                "plot_kernel",
                "--lasif_project=%s" % self.context["config"]["lasif_project"],
                "%s" % os.path.join(self.inputs["regular_model_folder"], key),
                "100",
                "--filename=%s" % os.path.join(self.working_dir, value),
                "--blockfile_folder=%s" % self.context["data_folder"]]
            self._run_external_script(cwd=".", cmd=cmd)

    def check_post_run(self):
        return
        # Copy the files
        for picture in self.outputs.values():
            src = os.path.join(self.working_dir, picture)
            dest = os.path.join(
                self.context["output_folders"]["regular_grid_models"], picture)
            shutil.copy2(src, dest)

    def generate_next_steps(self):
        # No next step for this job.
        pass
