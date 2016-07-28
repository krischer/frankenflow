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
        return {"local_binary_gradient_directory", "iteration_name"}

    def check_pre_staging(self):
        # The folder must exist!
        assert os.path.exists(
            self.inputs["local_binary_gradient_directory"]), \
            "'%s' does not exist" % self.inputs[
                "local_binary_gradient_directory"]

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
        self.filenames = []
        variables = ["grad_rho", "grad_csv", "grad_csh", "grad_cp"]
        for variable in variables:
            filename = "%s_%s_100km_depth.jpg" % (
                self.inputs["iteration_name"], variable)
            filename = os.path.join(self.working_dir, filename)
            self.filenames.append(filename)
            cmd = [
                self.context["config"]["lasif_cmd"],
                "plot_kernel",
                self.inputs["local_binary_gradient_directory"],
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
                    "ses3d_format_gradient_plots"],
                os.path.basename(src))
            shutil.copy2(src, dest)

    def generate_next_steps(self):
        # No next step for this job.
        pass
