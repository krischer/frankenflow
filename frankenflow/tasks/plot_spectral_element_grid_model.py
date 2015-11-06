import os
import shutil

from . import task


class PlotSpectralElementGridModel(task.Task):
    """
    Task plotting a regular grid model.
    """
    def check_pre_staging(self):
        assert "model_name" in self.inputs, "'model_name' must be part of " \
                                            "the inputs"

        folder = os.path.join(self.context["config"]["lasif_project"],
                              "MODELS", self.inputs["model_name"])
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
            filename = "%s_%s_100km_depth.jpg" % (
                self.inputs["model_name"], variable)
            filename = os.path.join(self.working_dir, filename)
            self.filenames.append(filename)
            cmd = [
                self.context["config"]["lasif_cmd"],
                "plot_model",
                self.inputs["model_name"],
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
                self.context["output_folders"]["spec_elem_grid_models"],
                os.path.basename(src))
            shutil.copy2(src, dest)

    def generate_next_steps(self):
        # No next step for this job.
        pass
