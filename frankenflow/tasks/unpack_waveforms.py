import os
import time

from . import task


class UnpackWaveforms(task.Task):
    """
    Unpack the waveforms to a certain LASIF project.
    """
    def check_pre_staging(self):
        self._assert_input_exists("model_name")
        self._assert_input_exists("tar_file")

        assert os.path.exists(self.inputs["tar_file"]), \
            "File '%s' does not yet exist." % self.inputs["tar_file"]

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        cmd = [self.c["agere_cmd"],
               "unpack_waveforms",
               "--iteration-name=%s" % self.inputs["model_name"],
               "--lasif-project=%s" % self.c["lasif_project"],
               self.inputs["tar_file"]]
        returncode = self._run_external_script(cwd=".", cmd=cmd)

        # Should be a good enough check.
        assert returncode == 0, "Script return with code %i" % returncode

    def check_post_run(self):
        pass

    def generate_next_steps(self):
        next_steps = [
            # Build the LASIF caches.
            {"task_type": "BuildLASIFCaches",
             # Just pass along the inputs
             "inputs": self.inputs,
             "priority": 0
             }
        ]
        return next_steps
