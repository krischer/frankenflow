import os

from . import task


class SumGradientsOnHPC(task.Task):
    """
    Sum the gradients on the HPC.

    This is a cheap operation and saves a lot of copy time and just easy to
    justify.
    """
    @property
    def required_inputs(self):
        return {"hpc_agere_bwd_job_id"}

    def check_pre_staging(self):
        self._init_ssh_and_stfp_clients()

        self.kernel_root_dir = os.path.join(
            self.c["hpc_agere_project"], "__KERNELS")
        self.hpc_kernel_directory = os.path.join(
            self.kernel_root_dir, self.inputs["hpc_agere_bwd_job_id"])

        # Make sure the kernels exist on the HPC.
        self.all_kernels = self.remote_listdir(self.hpc_kernel_directory)
        assert len(self.all_kernels) == self.c["number_of_events"], (
            "Run should have resulted in '%s' kernels." %
            self.c["number_of_events"])

        self.output_folder = os.path.join(
            self.kernel_root_dir,
            self.inputs["hpc_agere_bwd_job_id"] + "__SUMMED")

        contents = self.remote_listdir(self.kernel_root_dir)
        assert os.path.basename(self.output_folder) not in contents, \
            "Folder '%s' on HPC already exists." % self.output_folder

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        cmd = ("{agere} sum_spectral_element_grid_gradients "
               "--output_folder={output_folder} "
               "{kernels}").format(
            agere=self.c["hpc_agere_cmd"],
            output_folder=self.output_folder,
            kernels=os.path.join(self.hpc_kernel_directory, "*"))

        stdout, stderr = self._run_ssh_command(cmd)

        with open(self.stdout, "at") as fh:
            fh.write("\n\n")
            fh.write("".join(stdout))
            fh.write("\n\n")

        with open(self.stderr, "at") as fh:
            fh.write("\n\n")
            fh.write("".join(stderr))
            fh.write("\n\n")

    def check_post_run(self):
        # Make sure the folder did get created.
        contents = self.remote_listdir(self.kernel_root_dir)
        assert os.path.basename(self.output_folder) in contents, \
            "Folder '%s' on HPC " \
            "containing the summed kernel does not exist." % self.output_folder

        # And that it has at least 4 files.
        contents = self.remote_listdir(self.output_folder)
        assert len(contents) >= 4, "The folder '%s' on HPC which should " \
            "contain the summed kernel contains less then 4 files" % (
            self.output_folder)

    def generate_next_steps(self):
        next_steps = [
            {"task_type": "CopyGradientsFromHPC",
             "inputs": {
                 "summed_kernel_directory": self.output_folder
             },
             "priority": 0
             }
        ]
        return next_steps
