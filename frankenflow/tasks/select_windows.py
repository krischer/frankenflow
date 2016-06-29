from . import task


class SelectWindows(task.Task):
    """
    Select windows for iteration 0.
    """
    @property
    def required_inputs(self):
        return {"iteration_name"}

    def check_pre_staging(self):
        pass

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        cmd = ["mpirun", "-n", "4", self.c["lasif_cmd"], "select_all_windows",
               self.inputs["iteration_name"]]
        returncode = self._run_external_script(
            cwd=self.c["lasif_project"], cmd=cmd)

        # Should be a good enough check.
        assert returncode == 0, "Script return with code %i" % returncode

    def check_post_run(self):
        pass

    def generate_next_steps(self):
        # Calculate the misfit.
        next_steps = [
            {"task_type": "CalculateMisfit",
             "priority": 0
             }
        ]
        return next_steps
