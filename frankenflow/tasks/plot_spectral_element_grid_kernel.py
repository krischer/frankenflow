import os
import shutil

from . import task


class PlotSpectralElementGridGradient(task.Task):
    """
    Task plotting a regular grid gradient.
    """
    # No goal required for plotting. It is just a side activity.
    task_requires_active_goal = False

    @property
    def required_inputs(self):
        return {"model_name", "gradient_name", "summed_gradient_directory"}

    def check_pre_staging(self):
        assert os.path.exists(self.inputs["summed_gradient_directory"]), \
            "'%s' does not exist" %  self.inputs["summed_gradient_directory"]

        # boxfile from corresponding model
        self.boxfile = os.path.join(self.c["lasif_project"], "MODELS",
                                    self.inputs["model_name"], "boxfile")
        self.gradient_boxfile = os.path.join(
            self.inputs["summed_gradient_directory"], "boxfile")

        assert os.path.exists(self.boxfile), "File '%s' must exist." % \
            self.boxfile

    def stage_data(self):
        # Copy the boxfile from the corresponding model. Its really only
        # needed for the plotting so its fine to do that here.
        shutil.copy2(self.boxfile, self.gradient_boxfile)

    def check_post_staging(self):
        assert os.path.exists(self.gradient_boxfile), \
            "File '%s' does not exist." % self.gradient_boxfile

    def run(self):
        self.filenames = []
        variables = ["grad_rho", "grad_csv", "grad_csh", "grad_cp"]
        for variable in variables:
            filename = "%s_%s_100km_depth.jpg" % (
                self.inputs["gradient_name"], variable)
            filename = os.path.join(self.working_dir, filename)
            self.filenames.append(filename)
            cmd = [
                self.context["config"]["lasif_cmd"],
                "plot_kernel",
                self.inputs["summed_gradient_directory"],
                "100",
                variable,
                filename]

            self._run_external_script(
                cwd=self.context["config"]["lasif_project"],
                cmd=cmd)

    def check_post_run(self):
        # Copy the files
        for src in self.filenames:
            dest = os.path.join(
                self.context["output_folders"][
                    "unsmoothed_spectral_element_grid_gradients"],
                os.path.basename(src))
            shutil.copy2(src, dest)

    def generate_next_steps(self):
        # No next step for this job.
        pass
