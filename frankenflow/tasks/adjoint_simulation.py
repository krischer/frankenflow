import os
import re
import time

from . import task


class AdjointSimulation(task.Task):
    """
    Run an adjoint simulation.
    """
    @property
    def required_inputs(self):
        return {"remote_adjoint_source_directory",
                "hpc_agere_fwd_job_id"}

    def check_pre_staging(self):
        self._init_ssh_and_stfp_clients()

        # Model directory.
        self.remote_model_directory = os.path.join(
            self.context["config"]["hpc_agere_project"],
            "__MODELS")

        # Make sure the model directory exists.
        existing_models = self.sftp_client.listdir(self.remote_model_directory)
        assert self.inputs["model_name"] in existing_models, (
            "Model '%s' does not exist on the HPC" % (
                self.inputs["model_name"]))

        # Backwards id is forwards id with "_bw" suffix.
        self.hpc_agere_bwd_job_id = \
            re.sub("_fw$", "_bw", self.inputs["hpc_agere_fwd_job_id"])

        self.hpc_kernel_directory = os.path.join(
            self.c["hpc_agere_project"], "__KERNELS",
            self.hpc_agere_bwd_job_id)

        try:
            self.sftp_client.listdir(self.hpc_kernel_directory)
        except FileNotFoundError:
            pass
        else:
            raise ValueError("Folder '%s' exists on the remote host." % (
                self.hpc_kernel_directory))

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        cmd = ("{agere} run_adjoint --fw_run={fw_run} "
               "--wall-time-per-event={walltime_per_event} "
               "--parallel-events={parallel_events} {adjoint_srcs}").format(
            agere=self.c["hpc_agere_cmd"],
            fw_run=self.inputs["hpc_agere_fwd_job_id"],
            walltime_per_event=self.c["walltime_per_event_adjoint"],
            parallel_events=self.c["parallel_events"],
            adjoint_srcs=os.path.join(
                self.inputs["remote_adjoint_source_directory"], "*"))

        stdout, stderr = self._run_ssh_command(cmd)

        with open(self.stdout, "at") as fh:
            fh.write("\n\n")
            fh.write("".join(stdout))
            fh.write("\n\n")

        with open(self.stderr, "at") as fh:
            fh.write("\n\n")
            fh.write("".join(stderr))
            fh.write("\n\n")

        # Parse stdout to figure out the agere job number.
        for line in stdout:
            line = line.strip()
            if "Launching SES3D" in line:
                break
        else:
            raise ValueError("Could not find 'Launching SES3D' on stdout: %s"
                             % stdout)

        # Check if job is done all five minutes.
        while True:
            time.sleep(20)

            stdout, stderr = self._run_ssh_command(
                "%s status" % self.c["hpc_agere_cmd"])

            finished = False

            for line in stdout:
                line = line.strip()
                if line.startswith("JOB NUMBER") or line.startswith("====="):
                    continue
                line = line.split()
                if line[0] != self.hpc_agere_bwd_job_id:
                    continue

                status = line[1].upper()

                self.add_log_entry("Current status of remote job: %s" % status)

                if status == "FINISHED":
                    finished = True
                break
            else:
                raise ValueError("`agere_status` did not contain run '%s'" %
                                 self.hpc_agere_bwd_job_id)

            if not finished:
                continue
            else:
                break

    def check_post_run(self):
        # Make sure some kernels have been created.
        kernel_folder = self.sftp_client.listdir(self.hpc_kernel_directory)
        assert len(kernel_folder), "Run should have resulted in some kernels."

    def generate_next_steps(self):
        next_steps = [
            # Tar the waveforms.
            {"task_type": "CopyGradientsFromHPC",
             "inputs": {
                 "hpc_agere_bwd_job_id": self.hpc_agere_bwd_job_id
             },
             "priority": 0
             }
        ]
        return next_steps
