import os
import time

from . import task


class TarWaveformsOnHPC(task.Task):
    """
    Tar the waveforms on the HPC. This can easily take on hour.
    """
    @property
    def required_inputs(self):
        return {"hpc_agere_fwd_job_id"}

    def check_pre_staging(self):
        self._init_ssh_and_stfp_clients()

        self.expected_output_file = os.path.join(
            self.c["hpc_agere_project"], "__WAVEFORMS",
            "%s.tar" % self.inputs["hpc_agere_fwd_job_id"])

        # Make sure it does not yet exist.
        files = self.sftp_client.listdir(
            os.path.dirname(self.expected_output_file))
        assert os.path.basename(self.expected_output_file) not in files, \
            "File '%s' already exists." % (self.expected_output_file)

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        # This can take quite a bit of time ...
        stdout, stderr = self._run_ssh_command('%s tar_waveforms' %
                                               self.c["hpc_agere_cmd"])

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
            if self.expected_output_file in line:
                break
        else:
            raise ValueError("Did not tar the require agere run.")

    def check_post_run(self):
        # Make sure the file exists now.
        files = self.sftp_client.listdir(
            os.path.dirname(self.expected_output_file))
        assert os.path.basename(self.expected_output_file) in files, \
            "File '%s' has not been created." % (self.expected_output_file)

    def generate_next_steps(self):
        next_steps = [
            # Now copy the waveforms from the HPC.
            {"task_type": "CopyWaveformsFromHPC",
             "inputs": {
                 "remote_waveform_tar_file": self.expected_output_file,
             },
             "priority": 0
             }
        ]
        return next_steps
