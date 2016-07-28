import datetime
import json
import os
import re
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
        if "current_goal" in self.inputs:
            self.current_goal = self.inputs["current_goal"]
        else:
            self.current_goal = None

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    @property
    def seismopt_file(self):
        seismopt_next_file = os.path.join(
            self.context["seismopt_dir"], "seismopt.json")
        assert os.path.exists(seismopt_next_file), \
            "'%s' does not exist." % seismopt_next_file
        with open(seismopt_next_file, "rt") as fh:
            status = json.load(fh)
        status["_meta"]["iteration"] = int(status["_meta"]["iteration"])
        return status

    def run(self):
        # Having no current goal can be the result of two different things:
        #
        # 1. It is the very start of an inversion. In that case run seismopt.
        # 2. Seismopt did run the first time - in that case parse the
        #    seismopt file and set the next steps accordingly.
        if self.current_goal is None:
            try:
                status = self.seismopt_file
                if status["next_task"]["task_type"] != \
                        "calculate_misfit_and_gradient":
                    raise NotImplementedError
                self.misfit_and_gradient_goal()
            except AssertionError:
                self.setup_seismopt()
            return
        else:
            goal_type, iteration_name = self.current_goal.split()
            if goal_type == "misfit":
                self.misfit_and_gradient_goal()
            elif goal_type == "gradient":
                # This should be reached if we have a gradient and seismopt
                # now needs it as well as the misfit value.
                status = self.seismopt_file
                if status["next_task"]["task_type"] != \
                        "calculate_misfit_and_gradient":
                    raise NotImplementedError
                self.evaluate_misfit_and_gradient()
            else:
                print("================")
                print("================")
                print("================")
                print("================")
                print(goal_type, iteration_name)
                print("================")
                print("================")
                print("================")
                print("================")
                raise NotImplementedError

    def write_misfitfile_to_seismopt(self, iteration):
        # Read the misfit.
        misfit_file = self.get_misfit_file(iteration)
        assert os.path.exists(misfit_file)

        with open(misfit_file, "rt") as fh:
            misfit = float(fh.readline())

        output_file = os.path.join(
            self.context["seismopt_dir"], "MODEL_SES3D_H5", "misfit_model")
        with open(output_file, "wb") as fh:
            fh.write(struct.pack("d", misfit))

    def evaluate_misfit_and_gradient(self):
        status = self.seismopt_file

        # Get the iteration name and make sure seismopt and frankenflow are
        # on the same level.
        iteration_name = "%03i" % status["_meta"]["iteration"]
        assert iteration_name == self.current_goal.split()[1], \
            self.current_goal

        # At this point we have both, a misfit as well as a gradient - we
        # now have to pass this to seismopt which will determine what to do
        # next.
        self.write_misfitfile_to_seismopt(iteration=iteration_name)

        # Also copy the gradient.
        dst = os.path.join(
            self.context["seismopt_dir"], "MODEL_SES3D_H5", "gradient.h5")
        src = self.get_gradient_file(iteration_name)
        shutil.copy2(src=src, dst=dst)

        self.next_steps = [
            {"task_type": "RunSeismOpt",
             "inputs": {},
             "priority": 0
             }
        ]

    def misfit_and_gradient_goal(self):
        status = self.seismopt_file

        # Get the iteration name.
        iteration_name = "%03i" % status["_meta"]["iteration"]
        if "step_length" in status["_meta"]:
            iteration_name += \
                "_step_length_%.9f" % status["_meta"]["step_length"]

        # Check if the gradient already exists.
        gradient_exists = os.path.exists(self.get_gradient_file(
            iteration_name))
        # Check if the misfit already exists.
        misfit_exists = os.path.exists(self.get_misfit_file(iteration_name))

        # If only the gradient or both already exist then something is wrong.
        if not misfit_exists and gradient_exists:
            raise NotImplementedError("Only the gradient exists!")
        if misfit_exists and gradient_exists:
            raise NotImplementedError("Misfit and gradient already exist.")

        # Now, no matter what, the model files has to exist, otherwise it
        # has to be copied from the seismopt directory.
        model_file = self.get_model_file(iteration_name)
        if not os.path.exists(model_file):
            nt = status["next_task"]
            # Get the model from seismopt.
            filename = os.path.join(
                self.context["seismopt_dir"], nt["folder"].strip("./"),
                nt["prefix"] + os.extsep + "h5")
            assert os.path.exists(filename)
            shutil.copy2(filename, model_file)
        assert os.path.exists(model_file)

        # If the misfit does not exists, calculate it, start by converting
        # the model to the binary format.
        if not misfit_exists:
            self.new_goal = "misfit %s" % iteration_name
            self.next_steps = [
                {
                    "task_type": "ConvertModelToBinary",
                    "inputs": {"iteration_name": iteration_name},
                    "priority": 0
                },
                {
                    "task_type": "PlotHDF5Model",
                    "inputs": {"iteration_name": iteration_name},
                    "priority": 1
                }
            ]
        elif not gradient_exists:
            self.new_goal = "gradient %s" % iteration_name
            self.launch_adjoint_source_calculation()
        else:
            raise NotImplementedError

    def setup_seismopt(self):
        """
        This function is called when no goal yet exists -  it will setup the
        directory structure for seismopt and call it for the first time.
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
        self.next_steps = [
            {"task_type": "RunSeismOpt",
             "inputs": {},
             "priority": 0
            }
        ]

    def launch_adjoint_source_calculation(self):
        # Make sure the forward run is part of the inputs.
        self._assert_input_exists("hpc_agere_fwd_job_id")
        self._assert_input_exists("iteration_name")

        self.next_steps = [{
            "task_type": "CalculateAdjointSources",
            "inputs": {
                "iteration_name": self.inputs["iteration_name"],
                "hpc_agere_fwd_job_id": self.inputs["hpc_agere_fwd_job_id"]
            },
            "priority": 0
        }]

    def store_opt_next_file(self):
        """
        Store the current opt.next file to keep track of what is happening.
        """
        now = datetime.datetime.now()
        filename = now.strftime("%y%m%dT%H%M%S_") + "opt.next"

        target = os.path.join(
            self.context["output_folders"]["seismopt_next_files"], filename)

        shutil.copy2(self.seismopt_next, target)

    def misfit_goal(self, iteration_name):
        # Initial iteration_name. Now we also need the gradient. The first
        # step here is to calculate the adjoint sources.
        if iteration_name == "000":
            # Make sure the forward run is part of the inputs.
            self._assert_input_exists("hpc_agere_fwd_job_id")
            self.next_steps = [{
                "task_type": "CalculateAdjointSources",
                "inputs": {
                    "iteration_name": iteration_name,
                    "hpc_agere_fwd_job_id": self.inputs["hpc_agere_fwd_job_id"]
                },
                "priority": 0
            }]
            self.new_goal = "gradient %s" % iteration_name
        else:
            # Make sure the forward run is part of the inputs as it has to
            # passed on to the potential adjoint simulation.
            self._assert_input_exists("hpc_agere_fwd_job_id")

            # Make sure the opt.next file did not change.
            existing_ones = os.listdir(
                self.context["output_folders"]["seismopt_next_files"])
            # They are sorted by datetime, thus the last one is the latest one.
            latest_one = os.path.join(
                self.context["output_folders"]["seismopt_next_files"],
                sorted(existing_ones)[-1])
            current_one = self.seismopt_next

            with open(latest_one, "rt") as fh:
                latest_one = fh.read()

            with open(current_one, "rt") as fh:
                current_one = fh.read()

            assert latest_one == current_one, \
                "The latest archived opt.next file is not identical with " \
                "the current one. Thus it cannot be guaranteed that the " \
                "goal is still valid."

            contents = self.parse_current_seismopt_file()

            iteration, _, *name = iteration_name.split("_")
            iteration = "ITERATION_%s" % iteration

            # Make sure the iteration is consistent.
            assert iteration == contents["iteration"], "'%s' != '%s'" % (
                iteration, contents["iteration"])

            # Also the iteration_name name.
            assert iteration_name == contents["model_name"]

            # Assume its still valid. Write the misfit and run seimopt.
            self.write_misfit_to_opt(iteration, contents["prefix"], iteration_name)

            self.next_steps = [{
                "task_type": "RunSeismOpt",
                "inputs": {
                    "hpc_agere_fwd_job_id": self.inputs[
                        "hpc_agere_fwd_job_id"],
                    "model_name": iteration_name
                },
                "priority": 0
            }]


            return

    def gradient_goal(self, model):
        # Initial model. We thus have to setup the optimization structure.
        s_dir = self.context["seismopt_dir"]
        if model == "000_1_model":
            # The folder should not exist in the that case!
            os.makedirs(s_dir, exist_ok=False)

            # Copy the initial files.
            d_dir = self.context["data_folder"]
            shutil.copy2(os.path.join(d_dir, "seismopt/opt_settings.xml"),
                         os.path.join(s_dir, "opt_settings.xml"))
            shutil.copy2(os.path.join(d_dir, "seismopt/optlib.exe"),
                         os.path.join(s_dir, "optlib.exe"))
            shutil.copy2(os.path.join(d_dir, "seismopt/ses3d.cfg"),
                         os.path.join(s_dir, "ses3d.cfg"))

            self.copy_model_to_opt("ITERATION_000", "x", "000_1_model")
            self.copy_gradient_to_opt("ITERATION_000", "x", "000_2_gradient")
            self.write_misfit_to_opt("ITERATION_000", "x", "000_1_model")

            self.next_steps = [{
                "task_type": "RunSeismOpt",
                "priority": 0
            }]

        else:
            gradient = model.replace("_model_", "_gradient_")
            contents = self.parse_current_seismopt_file()
            self.copy_gradient_to_opt(contents["iteration"],
                                      contents["prefix"],
                                      gradient)

            # The misfit_and_gradient task also requires the misfit to be
            # written.
            if contents["task"] == "misfit_and_gradient":
                iteration, _, *name = model.split("_")
                iteration = "ITERATION_%s" % iteration

                # Make sure the iteration is consistent.
                assert iteration == contents["iteration"], "'%s' != '%s'" % (
                    iteration, contents["iteration"])

                # Also the model name.
                assert model == contents["model_name"]

                # Assume its still valid. Write the misfit and run seimopt.
                self.write_misfit_to_opt(iteration, contents["prefix"], model)

            self.next_steps = [{
                "task_type": "RunSeismOpt",
                "priority": 0
            }]

    def copy_model_to_opt(self, iteration, prefix, model_name):
        src_folder = os.path.join(
            self.context["optimization_dir"], model_name)
        assert os.path.exists(src_folder)

        expected_contents = {"x_rho", "x_vp", "x_vsh", "x_vsv"}
        actual_contents = set(os.listdir(src_folder))

        assert not expected_contents.difference(actual_contents)

        dest_folder = os.path.join(self.context["seismopt_dir"],
                                   iteration)
        os.makedirs(dest_folder, exist_ok=True)

        copy_map = {
            "x_rho": prefix + "_rho",
            "x_vp": prefix + "_vp",
            "x_vsh": prefix + "_vsh",
            "x_vsv": prefix + "_vsv",
        }

        for src, dest in copy_map.items():
            shutil.copy2(
                os.path.join(src_folder, src),
                os.path.join(dest_folder, dest))

    def copy_gradient_to_opt(self, iteration, prefix, gradient_name):
        src_folder = os.path.join(
            self.context["optimization_dir"], gradient_name)
        assert os.path.exists(src_folder), "Folder '%s' does not exist." % (
            src_folder)

        expected_contents = {"gradient_x_rho", "gradient_x_vp",
                             "gradient_x_vsh", "gradient_x_vsv"}
        actual_contents = set(os.listdir(src_folder))

        assert not expected_contents.difference(actual_contents), \
            "Expected: %s, Actual: %s" % (expected_contents,
                                          actual_contents)

        dest_folder = os.path.join(self.context["seismopt_dir"],
                                   iteration)
        os.makedirs(dest_folder, exist_ok=True)

        copy_map = {
            "gradient_x_rho": "gradient_" + prefix + "_rho",
            "gradient_x_vp": "gradient_" + prefix + "_vp",
            "gradient_x_vsh": "gradient_" + prefix + "_vsh",
            "gradient_x_vsv": "gradient_" + prefix + "_vsv",
        }

        for src, dest in copy_map.items():
            shutil.copy2(
                os.path.join(src_folder, src),
                os.path.join(dest_folder, dest))

    def check_post_run(self):
        try:
            if self.new_goal:
                return {"new_goal": self.new_goal}
            return {}
        except:
            return {}

    def generate_next_steps(self):
        return self.next_steps
