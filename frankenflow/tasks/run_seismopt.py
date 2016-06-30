import os

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
        pass

    def generate_next_steps(self):
        # It will pass on all inputs to the next stage.
        return [
            {"task_type": "Orchestrate",
             "priority": 0
            }
        ]
