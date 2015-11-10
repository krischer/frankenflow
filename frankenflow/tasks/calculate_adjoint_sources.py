import glob
import os

from . import task


class CalculateAdjointSources(task.Task):
    """
    Calculates the adjoint sources for a certain model.
    """
    @property
    def required_inputs(self):
        return ["model_name"]

    def check_pre_staging(self):
        self.events = self.get_events()

        ad_srcs = self.get_adjoint_source_folders()
        assert not ad_srcs, "Adjoint sources already exist"

    def get_adjoint_source_folders(self):
        # Make sure they don't yet exist.
        ad_src_folder = os.path.join(
            self.c["lasif_project"], "OUTPUT", "adjoint_sources")
        folders = glob.glob(os.path.join(
            ad_src_folder,
            "*__ITERATION_%s__*" % self.inputs["model_name"]))
        folders = [_i for _i in folders if os.path.isdir(_i)]
        return folders

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
        ad_srcs = self.get_adjoint_source_folders()
        assert ad_srcs, "No adjoint sources generated."

        for event in self.events:
            for folder in ad_srcs:
                if event in folder:
                    break
            else:
                raise ValueError("No adjoint source for event '%s' calculated"
                                 % event)

    def generate_next_steps(self):
        next_steps = []
        next_steps.append({
            "task_type": "CopyAdjointSourcesToHPC",
             "priority": 0
         })
        return next_steps
