from . import task


class MigrateWindows(task.Task):
    """
    Migrate windows from iteration 0 to current iteration.

    Will also create a new iteration.
    """
    def check_pre_staging(self):
        pass

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        # First create a new iteration starting from iteration 0.
        cmd = [self.c["lasif_cmd"],
               "create_successive_iteration 0 %s" %
               self._model_name_to_iteration(self.inputs["model_name"])]

        returncode = self._run_external_script(
            cwd=self.c["lasif_project"], cmd=cmd)
        assert returncode == 0, (
            "'create_successive_iteration' script return with code %i" %
            returncode)

        # Migrate the windows.
        cmd = [self.c["lasif_cmd"],
               "migrate_windows 0 %s" %
               self._model_name_to_iteration(self.inputs["model_name"])]

        returncode = self._run_external_script(
            cwd=self.c["lasif_project"], cmd=cmd)
        assert returncode == 0, (
            "'create_successive_iteration' script return with code %i" %
            returncode)

    def check_post_run(self):
        pass

    def generate_next_steps(self):
        # Calculate the misfit.
        next_steps = [
            {"task_type": "CalculateMisfit",
             # Just pass along the inputs
             "inputs": self.inputs,
             "priority": 0
             }
        ]
        return next_steps
