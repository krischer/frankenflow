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

        misfits = {}

        with open(self.stdout, "wt") as fh:
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
        pass

    def generate_next_steps(self):
        # XXX: Missing!
        pass
