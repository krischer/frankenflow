import os

from . import task


class SmoothAndPreconditionGradient(task.Task):
    """
    Smoothes and preconditions a gradient.
    """
    @property
    def required_inputs(self):
        return {"projected_gradient_folder", "gradient_name"}

    def check_pre_staging(self):
        assert os.path.exists(self.inputs["projected_gradient_folder"]), \
            "Directory '%s' does not exist." % self.inputs[
                "projected_gradient_folder"]

        # Store the gradients in the optimization directory.
        self.output_directory = os.path.join(
            self.context["optimization_dir"],
            self.inputs["gradient_name"])

        # Make sure this folder does not yet exist.
        assert not os.path.exists(self.output_directory), \
            "Directory '%s' already exists."


    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        cmd = [self.context["config"]["agere_cmd"],
               "smooth_and_precondition_gradient",
               "--smoothing-iterations=%i" % self.c["smoothing_iterations"],
               "--gradient_directory=%s" % self.output_directory,
               self.inputs["projected_gradient_folder"]]
        returncode = self._run_external_script(cwd=".", cmd=cmd)
        assert returncode == 0, \
            "Gradient preconditioning exited with return code %i." % returncode

    def check_post_run(self):
        expected_files = {"gradient_x_vp", "gradient_x_vsh", "gradient_x_vsv",
                          "gradient_x_rho"}
        actual_files = set(os.listdir(self.output_directory))

        difference = expected_files.difference(actual_files)

        assert not difference, \
            "The following files have not been created: %s" % (
                ", ".join(difference))

    def generate_next_steps(self):
        next_steps = [
            {"task_type": "Orchestrate",
             "priority": 0
            },
            {"task_type": "PlotRegularGridGradient",
             "priority": 1
             }
        ]
        return next_steps
