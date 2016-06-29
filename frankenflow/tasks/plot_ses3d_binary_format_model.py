import os
import shutil

from . import task


class PlotSES3DBinaryFormatModel(task.Task):
    """
    Task plotting a regular grid model.
    """
    # No goal required for plotting. It is just a side activity.
    task_requires_active_goal = False

    @property
    def required_inputs(self):
        return {"iteration_name"}

    def check_pre_staging(self):
        folder = self.binary_model_path
        assert os.path.exists(folder), "'%s' does not exist" % folder
        assert os.path.isdir(folder), "'%s' is not a folder" % folder

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        self.filenames = []
        variables = ["rho", "vsv", "vsh", "vp"]
        for variable in variables:
            filename = "model_%s_%s_100km_depth.jpg" % (
                self.inputs["iteration_name"], variable)
            filename = os.path.join(self.working_dir, filename)
            self.filenames.append(filename)
            cmd = [
                self.context["config"]["lasif_cmd"],
                "plot_model",
                os.path.basename(self.binary_model_path),
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
                self.context["output_folders"]["ses3d_format_models"],
                os.path.basename(src))
            shutil.copy2(src, dest)

    def generate_next_steps(self):
        # No next step for this job.
        pass
