import os
import time

from . import task


class CopyWaveformsFromHPC(task.Task):
    """
    Tar the waveforms on the HPC. This can easily take on hour.
    """
    def check_pre_staging(self):
        self._init_ssh_and_stfp_clients()

        # Make sure the model name is kept track of.
        assert "model_name" in self.inputs, "'model_name' must be part of " \
                                            "the inputs"
        self._assert_input_exists("model_name")
        self._assert_input_exists("job_number")
        self._assert_input_exists("tar_file")

        self.target_file = os.path.join(
            self.working_dir,
            os.path.basename(self.inputs["tar_file"]))

        # Make sure the tar file exists.
        files = self.sftp_client.listdir(
            os.path.dirname(self.inputs["tar_file"]))
        assert os.path.basename(self.inputs["tar_file"]) in files, \
            "Remote file '%s' does not exists." % (self.input["tar_file"])

        # And that the target file does not yet exist.
        assert not os.path.exists(self.target_file), \
            "File '%s' does already exist."

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        self.sftp_client.get(self.inputs["tar_file"], self.target_file)

    def check_post_run(self):
        # Make sure it exists now.
        assert os.path.exists(self.target_file), \
            "File '%s' does not exist."

    def generate_next_steps(self):
        next_steps = [
            # Now copy the waveforms from the HPC.
            {"task_type": "UnpackWaveforms",
             "inputs": {
                 "tar_file": self.target_file,
                 # Keep track of the current model and pass it from task to
                 # task.
                 "model_name": self.inputs["model_name"]
             },
             "priority": 0
             }
        ]
        return next_steps
