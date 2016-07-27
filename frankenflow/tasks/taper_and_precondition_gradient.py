import os

from . import task


class TaperAndPreconditionGradient(task.Task):
    """
    Tapers and preconditions a gradient.
    """
    @property
    def required_inputs(self):
        return {"iteration_name"}

    def check_pre_staging(self):
        self.hdf5_gradient_filename = \
            self.get_gradient_file(self.inputs["iteration_name"])

        assert os.path.exists(self.hdf5_gradient_filename), \
            "HDF5 gradient must exist!"

        raise Exception
        assert os.path.exists(self.inputs["projected_gradient_folder"]), \
            "Directory '%s' does not exist." % self.inputs[
                "projected_gradient_folder"]

        # Store the gradients in the optimization directory.
        self.output_directory = os.path.join(
            self.context["optimization_dir"],
            self.inputs["gradient_name"])

        # Make sure this folder does not yet exist.
        assert not os.path.exists(self.output_directory), \
            "Directory '%s' already exists." % self.output_directory


    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        cmd = [self.context["config"]["agere_cmd"],
               "smooth_and_precondition_gradient",
               "--smoothing-iterations=%i" % self.c["smoothing_iterations"],
               "--output_directory=%s" % self.output_directory,
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
            # Plot the processed gradient.
            {"task_type": "PlotHDF5Gradient",
             "inputs": {"tag": "tapered_and_preconditioned"},
             "priority": 1
             },
            # Orchestrate.
            {"task_type": "Orchestrate",
             "priority": 0
             }
        ]
        return next_steps
