import os
import time

from . import task


class UnpackWaveforms(task.Task):
    """
    Unpack the waveforms to a certain LASIF project.
    """
    @property
    def required_inputs(self):
        return ["waveform_tar_file", "model_name"]

    def check_pre_staging(self):
        assert os.path.exists(self.inputs["waveform_tar_file"]), \
            "File '%s' does not yet exist." % self.inputs["waveform_tar_file"]

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        cmd = [self.c["agere_cmd"],
               "unpack_waveforms",
               "--iteration-name=%s" % self.inputs["model_name"],
               "--lasif-project=%s" % self.c["lasif_project"],
               self.inputs["waveform_tar_file"]]
        returncode = self._run_external_script(cwd=".", cmd=cmd)

        # Should be a good enough check.
        assert returncode == 0, "Script return with code %i" % returncode

    def check_post_run(self):
        pass

    def generate_next_steps(self):
        next_steps = [
            # Build the LASIF caches.
            {"task_type": "BuildLASIFCaches",
             "priority": 0
             }
        ]
        return next_steps
