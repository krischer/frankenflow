import os
import time

from . import task


class CopyGradientsFromHPC(task.Task):
    """
    Tar the waveforms on the HPC. This can easily take on hour.
    """
    @property
    def required_inputs(self):
        return {"summed_kernel_directory"}

    def check_pre_staging(self):
        self._init_ssh_and_stfp_clients()

        self.local_kernel_directory = os.path.join(
            self.working_dir, "KERNELS")

        # Make sure the kernels exist on the HPC.
        self.kernel_files = self.remote_listdir(
            self.inputs["summed_kernel_directory"])
        assert len(self.kernel_files) >= 4, \
            "At least 4 kernel files should be available"

        # But not yet on the local machine.
        assert not os.path.exists(self.local_kernel_directory), \
            "Directory '%s' already exists." % self.local_kernel_directory

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        cmd = ["rsync", "-aP",
               "%s:%s/" % (self.c["hpc_remote_host"],
                           self.inputs["summed_kernel_directory"]),
               self.local_kernel_directory]

        retcode = self._run_external_script(cwd=".", cmd=cmd)
        assert retcode == 0, "rsync encountered an error."

    def check_post_run(self):
        # Make sure the kernels have been copied.
        kernel_folder = os.listdir(self.local_kernel_directory)
        assert set(self.kernel_files) == set(kernel_folder), \
            "Not all kernel files where copied successfully."

    def generate_next_steps(self):
        next_steps = [
            # Sum the gradients.
            {"task_type": "ConvertGradientsToHDF5",
             "inputs": {
                 "local_binary_gradient_directory": self.local_kernel_directory
             },
             "priority": 0
             },
            {"task_type": "PlotSpectralElementGridGradient",
             "inputs": {
                 "local_binary_gradient_directory": self.local_kernel_directory
             },
             "priority": 1
             }
        ]
        return next_steps
