import os
import time

from . import task


class CopyGradientsFromHPC(task.Task):
    """
    Tar the waveforms on the HPC. This can easily take on hour.
    """
    @property
    def required_inputs(self):
        return {"hpc_agere_bwd_job_id"}

    def check_pre_staging(self):
        c = self.context["config"]

        self._init_ssh_and_stfp_clients()

        self.hpc_kernel_directory = os.path.join(
            self.c["hpc_agere_project"], "__KERNELS",
            self.inputs["hpc_agere_bwd_job_id"])

        self.local_kernel_directory = os.path.join(
            self.working_dir, "KERNELS")

        # Make sure the kernels exist on the HPC.
        kernel_folder = self.remote_listdir(self.hpc_kernel_directory)
        assert len(kernel_folder) == c["number_of_events"], \
            "Run should have resulted in '%s' kernels." % c["number_of_events"]

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
                           self.hpc_kernel_directory),
               self.local_kernel_directory]

        self._run_external_script(cwd=".", cmd=cmd)

    def check_post_run(self):
        c = self.context["config"]

        # Make sure the kernels have been copied.
        kernel_folder = os.listdir(self.local_kernel_directory)
        assert len(kernel_folder) == c["number_of_events"], \
            "Run should have resulted in '%s' kernels being copied" % c[
                "number_of_events"]

    def generate_next_steps(self):
        next_steps = [
            # Sum the gradients.
            {"task_type": "SumGradients",
             "inputs": {
                 "local_kernel_directory": self.local_kernel_directory
             },
             "priority": 0
             }
        ]
        return next_steps
