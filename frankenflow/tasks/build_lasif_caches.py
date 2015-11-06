from . import task


class BuildLASIFCaches(task.Task):
    """
    Unpack the waveforms to a certain LASIF project.
    """
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
        if self._model_name_to_iteration(self.inputs["model_name"]) == "0":
            next_steps.append({
                "task_type": "SelectWindows",
                 # Just pass along the inputs
                 "inputs": self.inputs,
                 "priority": 0
             })
        else:
            next_steps.append({
                "task_type": "MigrateWindows",
                # Just pass along the inputs
                "inputs": self.inputs,
                "priority": 0
            })
        return next_steps
