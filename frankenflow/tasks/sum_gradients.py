import os

from . import task


class SumGradients(task.Task):
    """
    Sum the gradients on the spectral element grid.
    """
    @property
    def required_inputs(self):
        return {"local_kernel_directory"}

    def check_pre_staging(self):
        kernel_folder = os.listdir(self.inputs["local_kernel_directory"])
        assert len(kernel_folder) == self.c["number_of_events"], \
            "'%s' kernels should be available." % \
            self.c["number_of_events"]

        self.summed_kernel_directory = os.path.join(
            self.working_dir, "SUMMED_KERNEL")
        assert not os.path.exists(self.summed_kernel_directory), \
            "Directory '%s' already exists." % self.summed_kernel_directory

        self.local_kernel_folders = [
            os.path.join(self.inputs["local_kernel_directory"], _i)
            for _i in kernel_folder]

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        cmd = [self.c["agere_cmd"],
               "sum_spectral_element_grid_gradients",
               "--output_folder=%s" % self.summed_kernel_directory,
               *self.local_kernel_folders]
        returncode = self._run_external_script(cwd=".", cmd=cmd)

        # Should be a good enough check.
        assert returncode == 0, "Script return with code %i" % returncode

    def check_post_run(self):
        self.summed_kernel_directory = os.path.join(
            self.working_dir, "SUMMED_KERNEL")
        assert os.path.exists(self.summed_kernel_directory), \
            "Directory '%s' does not exists." % self.summed_kernel_directory

        contents = os.listdir(self.summed_kernel_directory)
        assert contents, "Folder '%s' does not have any contents." % \
            self.summed_kernel_directory

    def generate_next_steps(self):
        next_steps = [
            # Plot the unsmoothed spectral element kernels.
            {"task_type": "PlotSpectralElementGridGradient",
             "inputs": {
                 "summed_kernel_directory": self.summed_kernel_directory
             },
             "priority": 1
             },
            # And project the summed kernel to the regular grid.
            {"task_type": "ProjectGradient",
             "inputs": {
                 "summed_kernel_directory": self.summed_kernel_directory
             },
             "priority": 0
             }
        ]
        return next_steps
