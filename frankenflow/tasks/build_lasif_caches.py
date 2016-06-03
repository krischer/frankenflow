from . import task


class BuildLASIFCaches(task.Task):
    """
    Unpack the waveforms to a certain LASIF project.
    """
    @property
    def required_inputs(self):
        return {"model_name"}

    def check_pre_staging(self):
        pass

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        cmd = [self.c["lasif_cmd"], "build_all_caches", "--quick"]
        returncode = self._run_external_script(
            cwd=self.c["lasif_project"], cmd=cmd)

        # Should be a good enough check.
        assert returncode == 0, "Script return with code %i" % returncode

    def check_post_run(self):
        pass

    def generate_next_steps(self):
        next_steps = []
        # Now we need to either migrate the windows or pick new ones.
        # We'll only pick new ones if the current iteration is iteration 0.
        if self.inputs["model_name"] == "000_model":
            next_steps.append({
                "task_type": "SelectWindows",
                 "priority": 0
             })
        else:
            next_steps.append({
                "task_type": "MigrateWindows",
                "priority": 0
            })
        return next_steps
