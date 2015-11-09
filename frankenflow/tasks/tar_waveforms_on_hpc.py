import os
import time

from . import task


class TarWaveformsOnHPC(task.Task):
    """
    Tar the waveforms on the HPC. This can easily take on hour.
    """
    def check_pre_staging(self):
        self._init_ssh_and_stfp_clients()

        # Make sure the model name is kept track of.
        assert "model_name" in self.inputs, "'model_name' must be part of " \
                                            "the inputs"

        self.inputs["model_name"] = self.inputs["model_name"].lower()

        assert "job_number" in self.inputs, "'job_number' must be part of " \
                                            "the inputs"

        self.expected_output_file = os.path.join(
            self.c["hpc_agere_project"], "__WAVEFORMS",
            "%s.tar" % self.inputs["job_number"])

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
                 "job_number": self.inputs["job_number"],
                 "tar_file": self.expected_output_file,
                 # Keep track of the current model and pass it from task to
                 # task.
                 "model_name": self.inputs["model_name"]
             },
             "priority": 0
             }
        ]
        return next_steps
