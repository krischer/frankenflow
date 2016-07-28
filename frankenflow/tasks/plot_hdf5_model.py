import os
import shutil

from . import task
from .. import utils


class PlotHDF5Model(task.Task):
    """
    Very simple task plotting all fields of an HDF5 model.
    """
    # No goal required for plotting. It is just a side activity.
    task_requires_active_goal = False

    @property
    def required_inputs(self):
        return {"iteration_name"}

    def check_pre_staging(self):
        filename = self.hdf5_model_path
        assert os.path.exists(filename), "'%s' does not exist" % filename

        model_name = os.path.splitext(os.path.basename(filename))[0]

        # Make sure the output files don't.
        self.outputs = {
            "rho": model_name + "_rho.jpg",
            "vp": model_name + "_vp.jpg",
            "vsh": model_name + "_vsh.jpg",
            "vsv": model_name + "_vsv.jpg"
        }

        utils.assert_files_dont_exist(self.working_dir,
                                      list(self.outputs.values()))

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        for key, value in self.outputs.items():
            cmd = [
                self.context["config"]["agere_cmd"],
                "plot_hdf5",
                self.hdf5_model_path,
                key,
                os.path.join(self.working_dir, value)]
            self._run_external_script(cwd=".", cmd=cmd)

    def check_post_run(self):
        # Copy the files
        for picture in self.outputs.values():
            src = os.path.join(self.working_dir, picture)
            dest = os.path.join(
                self.context["output_folders"]["hdf5_models"], picture)
            shutil.copy2(src, dest)

    def generate_next_steps(self):
        # No next step for this job.
        pass
