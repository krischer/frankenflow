from . import task


class CalculateMisfit(task.Task):
    """
    Calculate the misfit.
    """
    def check_pre_staging(self):
        pass

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        cmd = ["mpirun", "-n", "4", self.c["lasif_cmd"],
               "compare_misfits","0",
               self._model_name_to_iteration(self.inputs["model_name"])]

        returncode = self._run_external_script(
            cwd=self.c["lasif_project"], cmd=cmd)
        assert returncode == 0, "Script return with code %i" % returncode

    def check_post_run(self):
        pass

    def generate_next_steps(self):
        # XXX: Missing!
        pass
