import json
import os
import shutil
import struct

from . import task


class Orchestrate(task.Task):
    """
    Orchestrate node always called when its no exactly clear what the next
    step will be.
    """
    # No goal required for orchestration. The whole point of the orchestrate
    # node is to assign a new goal.
    task_requires_active_goal = False

    @property
    def required_inputs(self):
        return set()

    def check_pre_staging(self):
        pass

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    @property
    def seismopt_file(self):
        """
        Get and parse the current seismopt file.

        Returns None if the file does not yet exist.
        """
        seismopt_next_file = os.path.join(
            self.context["seismopt_dir"], "seismopt.json")

        if not os.path.exists(seismopt_next_file):
            return None

        with open(seismopt_next_file, "rt") as fh:
            status = json.load(fh)
        status["_meta"]["iteration"] = int(status["_meta"]["iteration"])
        return status

    def run(self):
        status = self.seismopt_file

        # File does not yet exists - e.g. this is the very first seismopt run.
        if status is None:
            self.setup_seismopt()
            return

        # Check the status.
        assert status["current_run"] == "success", \
            "Status of seismopt is '%s'. Check whats up here!" % status[
                "current_run"]

        known_task_types = ["calculate_misfit", "calculate_gradient",
                            "calculate_misfit_and_gradient"]
        tt = status["next_task"]["task_type"]
        if tt not in known_task_types:
            raise Exception("Task type '%s' now known." % tt)

        # It all revolves around models and misfits. First check which one
        # we are talking about.
        iteration = "%03i" % int(status["_meta"]["iteration"])
        if "step_length" in status["_meta"]:
            iteration += "_step_length_" + status["_meta"]["step_length"]

        # Now we check if the model, misfit and/or gradient are existing.
        model = self.get_model_file(iteration)
        misfit = self.get_misfit_file(iteration)
        gradient = self.get_gradient_file(iteration, tag="preconditioned")

        # Now, no matter what, the model files has to exist, otherwise it
        # has to be copied from the seismopt directory.
        if not os.path.exists(model):
            nt = status["next_task"]
            # Get the model from seismopt.
            filename = os.path.join(
                self.context["seismopt_dir"], nt["folder"].strip("./"),
                nt["prefix"] + os.extsep + "h5")
            assert os.path.exists(filename)
            shutil.copy2(filename, model)
        assert os.path.exists(model)

        misfit_exists = os.path.exists(misfit)
        gradient_exists = os.path.exists(gradient)

        if not misfit_exists and gradient_exists:
            msg = "Unexpected state: misfit '%s' does not exists, but the " \
                  "gradient '%s' does." % (misfit, gradient)
            raise Exception(msg)

        # If the misfit does not exists, calculate it, start by converting
        # the model to the binary format.
        if "misfit" in tt and not misfit_exists:
            self.new_goal = "misfit %s" % iteration
            self.next_steps = [
                {
                    "task_type": "ConvertModelToBinary",
                    "inputs": {"iteration_name": iteration},
                    "priority": 0
                },
                {
                    "task_type": "PlotHDF5Model",
                    "inputs": {"iteration_name": iteration},
                    "priority": 1
                }
            ]
            return
        elif "gradient" in tt and not gradient_exists:
            self.new_goal = "gradient %s" % iteration
            # Make sure the forward run is part of the inputs.
            self._assert_input_exists("hpc_agere_fwd_job_id")

            self.next_steps = [{
                "task_type": "CalculateAdjointSources",
                "inputs": {
                    "iteration_name": iteration,
                    "hpc_agere_fwd_job_id": self.inputs["hpc_agere_fwd_job_id"]
                },
                "priority": 0
            }]
            return

        if "misfit" in tt:
            self.write_misfit_file_to_seismopt(iteration)

        if "gradient" in tt:
            self.write_gradient_to_seismopt(filename=gradient)

            # The gradient requires the id of the forward run - it always
            # has to exist - pass it on to seismopt which will carry it along.
            self._assert_input_exists("hpc_agere_fwd_job_id")
            self.inputs = {
                "hpc_agere_fwd_job_id": self.inputs["hpc_agere_fwd_job_id"]
            }
        else:
            self.inputs = {}


        # Now just run seismop and see what happens.
        self.new_goal = None
        self.next_steps = [
            {"task_type": "RunSeismOpt",
             "inputs": self.inputs,
             "priority": 0
             }
        ]

    def write_gradient_to_seismopt(self, filename):
        dst = os.path.join(
            self.context["seismopt_dir"],
            self.seismopt_file["next_task"]["folder"],
            "gradient.h5")
        shutil.copy2(src=filename, dst=dst)

    def write_misfit_file_to_seismopt(self, iteration):
        # Read the misfit.
        misfit_file = self.get_misfit_file(iteration)
        assert os.path.exists(misfit_file)

        with open(misfit_file, "rt") as fh:
            misfit = float(fh.readline())

        output_file = os.path.join(
            self.context["seismopt_dir"],
            self.seismopt_file["next_task"]["folder"],
            "misfit_model")
        with open(output_file, "wb") as fh:
            fh.write(struct.pack("d", misfit))

    def setup_seismopt(self):
        """
        This functions sets up seismopt for the very first run.
        """
        opt_dir = self.context["seismopt_dir"]
        assert os.path.exists(opt_dir), "Optimization directory does not exist"
        assert not os.listdir(opt_dir), "Optimization directory '%s' " \
            "must be empty!" % opt_dir

        # This only requires a couple of things: The model, the seismopt
        # executable and two config files.
        _i = self.context["data_folder"]
        model_file = os.path.join(_i, "000_model.h5")
        assert os.path.exists(model_file)

        executable = os.path.join(_i, "optlib.exe")
        assert os.path.exists(executable)

        for _i in [model_file, executable]:
            shutil.copy2(_i, os.path.join(opt_dir, os.path.basename(_i)))

        # Generate the config files.
        ses3d_cfg = [
            "[data_fields]",
            "field = rho",
            "field = vp",
            "field = vsv",
            "field = vsh",
            "",
            "[prefix]",
            "model = model",
            "misfit = misfit",
            "gradient = gradient",
            "search_direction = s",
            "tmp_model = model_n",
            "",
            "[smoothing]",
            "sigma_theta = %.8f" % self.c["sigma_theta"],
            "sigma_phi = %.8f" % self.c["sigma_phi"],
            "sigma_r = %.8f" % self.c["sigma_r"]
        ]
        with open(os.path.join(opt_dir, "ses3d.cfg"), "wt") as fh:
            fh.write("\n".join(ses3d_cfg))

        opt_settings = [
            "<opt_settings>",
            "    <path_to_initial_model>000_model</path_to_initial_model>",
            "    <working_directory>.</working_directory>",
            "    <max_relative_model_change>%f""</max_relative_model_change>" %
            self.c["max_relative_model_change"],
            "</opt_settings>"
        ]
        with open(os.path.join(opt_dir, "opt_settings.xml"), "wt") as fh:
            fh.write("\n".join(opt_settings))

        # Now run seismopt for the first time.
        self.new_goal = None
        self.next_steps = [
            {"task_type": "RunSeismOpt",
             "inputs": {},
             "priority": 0
            }
        ]

    def check_post_run(self):
        try:
            if hasattr(self, "new_goal"):
                return {"new_goal": self.new_goal}
            return {}
        except:
            return {}

    def generate_next_steps(self):
        return self.next_steps
