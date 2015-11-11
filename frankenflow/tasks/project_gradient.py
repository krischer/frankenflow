import os
import shutil

from . import task
from .. import utils


class ProjectGradient(task.Task):
    """
    Task projecting a gradient to a regular grid.
    """
    @property
    def required_inputs(self):
        return {"summed_gradient_directory", "gradient_name", "model_name"}

    def check_pre_staging(self):
        assert os.path.exists(self.inputs["summed_gradient_directory"]), \
            "Directory '%s' does not exist."

        self.output_directory = os.path.join(
            self.working_dir, "PROJECTED_GRADIENT")

        # We get the boxfile from the model and thus we also need to make
        # sure it exists.
        self.boxfile = os.path.join(self.c["lasif_project"], "MODELS",
                                    self.inputs["model_name"], "boxfile")
        assert os.path.exists(self.boxfile), "File '%s' must exist." % \
                                             self.boxfile

        assert not os.path.exists(self.output_directory), \
            "Output directory '%s' already exists." % self.output_directory

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        cmd = [self.context["config"]["agere_cmd"],
               "project_gradient",
               "--verbose",
               "--blockfile_folder=%s" % self.context["data_folder"],
               "--boxfile=%s" % self.boxfile,
               "--output_folder=%s" % self.output_directory,
               self.inputs["summed_gradient_directory"]]
        returncode = self._run_external_script(cwd=".", cmd=cmd)
        assert returncode == 0, \
            "Kernel projection exited with return code %i." % returncode

    def check_post_run(self):
        # We need to make sure that the expected files have been generated.
        self.projected_gradient_folder = os.path.join(
            self.output_directory, os.path.basename(
                self.inputs["summed_gradient_directory"]))

        expected_files = {"block_x", "block_y", "block_z", "gradient_cp",
                          "gradient_csh", "gradient_csv", "gradient_rho"}

        actual_files = set(os.listdir(self.projected_gradient_folder))

        difference = expected_files.difference(actual_files)

        assert not difference, \
            "The following files have not been created: %s" % (
                ", ".join(difference))

    def generate_next_steps(self):
        next_steps = [
            # Produce a plot of the projected model.
            {"task_type": "SmoothAndPreconditionGradient",
             "inputs": {
             },
             "priority": 0
            }
        ]
        return next_steps
