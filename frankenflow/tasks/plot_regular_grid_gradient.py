import os
import shutil

from . import task
from .. import utils


class PlotRegularGridGradient(task.Task):
    """
    Task plotting a regular grid gradient.
    """
    # No goal required for plotting. It is just a side activity.
    task_requires_active_goal = False

    @property
    def required_inputs(self):
        return {"gradient_name"}

    def check_pre_staging(self):
        # Store the gradients in the optimization directory.
        self.gradient_directory = os.path.join(
            self.context["optimization_dir"],
            self.inputs["gradient_name"])

        # Make sure this folder exists.
        assert  os.path.exists(self.gradient_directory), \
            "Directory '%s' does not exist." % self.gradient_directory

        expected_files = {"gradient_x_vp", "gradient_x_vsh", "gradient_x_vsv",
                          "gradient_x_rho"}
        actual_files = set(os.listdir(self.gradient_directory))

        difference = expected_files.difference(actual_files)

        assert not difference, "File are missing: %s" % ", ".join(difference)

        gn = self.inputs["gradient_name"]

        self.outputs = {
            "gradient_x_vp": gn + "_vp_100km_depth.jpg",
            "gradient_x_vsh": gn + "_vsh_100km_depth.jpg",
            "gradient_x_vsv": gn + "_vsv_100km_depth.jpg",
            "gradient_x_rho": gn + "_rho_100km_depth.jpg"
        }

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        for key, value in self.outputs.items():
            cmd = [
                self.context["config"]["agere_cmd"],
                "plot_kernel",
                "--lasif_project=%s" % self.context["config"]["lasif_project"],
                "%s" % os.path.join(self.gradient_directory, key),
                "100",
                "--filename=%s" % os.path.join(self.working_dir, value),
                "--blockfile_folder=%s" % self.context["data_folder"]]
            self._run_external_script(cwd=".", cmd=cmd)

    def check_post_run(self):
        # Copy the files
        for picture in self.outputs.values():
            src = os.path.join(self.working_dir, picture)
            dest = os.path.join(
                self.context["output_folders"]["regular_grid_gradients"],
                picture)
            shutil.copy2(src, dest)

    def generate_next_steps(self):
        # No next step for this job.
        pass
