import datetime
import os
import shutil

from . import task


class RunSeismOpt(task.Task):
    """
    Node running seismopt.

    Its not the brightest star on the horizon as far as nodes go: It just runs
    seismopt and then returns to the orchestration node.
    """
    @property
    def required_inputs(self):
        return set()

    def check_pre_staging(self):
        self.executable = os.path.join(self.context["seismopt_dir"],
                                       "optlib.exe")
        assert os.path.exists(self.executable)

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        cmd = [self.executable]
        returncode = self._run_external_script(
            cwd=self.context["seismopt_dir"],
            cmd=cmd)
        assert returncode in (0, 1), \
            "seismopt exited with return code %i." % returncode

    def check_post_run(self):
        # After each run, copy a timestamped seimsopt file to the outputs.
        # This should enable to reconstruct what seismopt has been up to.
        src = os.path.join(
            self.context["seismopt_dir"], "seismopt.json")
        dst = os.path.join(
            self.context["output_folders"],
            "seismopt_json_files",
            datetime.datetime.now().strftime("%y%m%dT%H%M%S_") +
            "_seismopt.json")
        shutil.copy2(src=src, dst=dst)

    def generate_next_steps(self):
        # It will pass on all inputs to the next stage.
        return [
            {"task_type": "Orchestrate",
             "priority": 0
            }
        ]
