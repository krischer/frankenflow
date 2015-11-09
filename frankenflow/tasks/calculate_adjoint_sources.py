from . import task


class CalculateAdjointSources(task.Task):
    """
    Calculates the adjoint sources for a certain model.
    """
    def check_pre_staging(self):
        self._assert_input_exists("model_name")
        self.events = self.get_events()

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        for event in self.events:
            cmd = [self.c["lasif_cmd"], "finalize_adjoint_sources",
                   self.inputs["model_name"], event]
            returncode = self._run_external_script(
                cwd=self.c["lasif_project"], cmd=cmd)

            # Should be a good enough check.
            assert returncode == 0, "Script return with code %i" % returncode

    def check_post_run(self):
        pass

    def generate_next_steps(self):
        next_steps = []
        next_steps.append({
            "task_type": "CopyAdjointSourcesToHPC",
             # Just pass along the inputs
             "inputs": self.inputs,
             "priority": 0
         })
