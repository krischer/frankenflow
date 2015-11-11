import os

from . import task


class CalculateMisfit(task.Task):
    """
    Calculate the misfit.
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
        # For a reason I don't understand sometimes not all caches are
        # built. This is quick (at most a minute) as the cache generation
        # script has been run before.
        cmd = [self.c["lasif_cmd"], "build_all_caches", "--quick"]
        returncode = self._run_external_script(
            cwd=self.c["lasif_project"], cmd=cmd)

        # Should be a good enough check.
        assert returncode == 0, "Script return with code %i" % returncode


        cmd = ["mpirun", "-n", "4", self.c["lasif_cmd"],
               "compare_misfits","000_1_model", self.inputs["model_name"]]

        returncode = self._run_external_script(
            cwd=self.c["lasif_project"], cmd=cmd)
        assert returncode == 0, "Script return with code %i" % returncode

        misfits = {}

        with open(self.stdout, "rt") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                line = line.lower()
                if not line.startswith("total misfit"):
                    continue

                line = line.lstrip("total misfit in iteration").strip()

                iteration, misfit = line.split(":")
                iteration = iteration.strip()
                misfit = float(misfit)

                misfits[iteration] = misfit

                self.add_log_entry("Misfit for iteration %s: %g" % (
                    iteration, misfit))

        assert misfits, "Could not capture the calculated misfits"

        self.misfits = misfits

    def check_post_run(self):
        # Store the misfits as they are kind of important.
        misfit_folder = os.path.join(
            self.context, self.context["output_folders"]["misfits"])

        for key, value in self.misfits.items():
            filename = os.path.join(misfit_folder, "iteration_%s.txt" % key)

            if os.path.exists(filename):
                if key != "000_1_model":
                    raise ValueError(
                        "Misfit for iteration %s already exists!" % key)
                continue

            with open(filename, "wt") as fh:
                fh.write("%g" % value)

    def generate_next_steps(self):
        # This not requires an orchestration to figure out what to do next.
        next_steps = [
            {"task_type": "Orchestrate",
             "priority": 0
             }
        ]
        return next_steps
