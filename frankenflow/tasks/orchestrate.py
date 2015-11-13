import datetime
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
        return {"current_goal"}

    def check_pre_staging(self):
        self.current_goal = self.inputs["current_goal"]

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        # If no current goal is set, evaluate the result of seismopt and act
        # accordingly.
        if self.current_goal is None:
            self.steer_with_seismopt()
            return

        goal_type, model = self.current_goal.split()

        if goal_type == "misfit":
            self.misfit_goal(model)
        elif goal_type == "gradient":
            self.gradient_goal(model)
        elif goal_type == "misfit_and_gradient":
            self.misfit_and_gradient_goal(model)
        else:
            raise NotImplementedError

    @property
    def seismopt_next(self):
        seismopt_next_file = os.path.join(
            self.context["seismopt_dir"], "opt.next")
        assert os.path.exists(seismopt_next_file), \
            "'%s' does not exist." % seismopt_next_file
        return seismopt_next_file

    def parse_current_seismopt_file(self):
        with open(self.seismopt_next, "rt") as fh:
            line_1 = fh.readline().strip()
            line_2 = fh.readline().strip()

        prog, task, iteration, prefix = line_2.split()

        # Folder with all the things.
        folder = os.path.normpath(os.path.join(self.context["seismopt_dir"],
                                               iteration))
        number = re.findall("\d+", iteration)[0]

        assert prog == "run_ses3d"

        contents = {
            "task": task,
            "iteration": os.path.normpath(iteration),
            "prefix": prefix,
            "folder": folder,
            "number": number
        }

        if task == "misfit":
            # If the misfit is requested, the desired step length has to be
            # given.
            assert "test step length" in line_1
            # Step length
            step_length = float(line_1.split()[-1])
            contents["step_length"] = step_length

            model_name = "%s_x_model_steplength_%g" % (number, step_length)
            contents["model_name"] = model_name

        elif task == "misfit_and_gradient":
            # If the misfit is requested, the desired step length has to be
            # given.
            assert "test step length" in line_1
            # Step length
            step_length = float(line_1.split()[-1])
            contents["step_length"] = step_length

            model_name = "%s_x_model_steplength_%g" % (number, step_length)
            contents["model_name"] = model_name

        return contents

    def steer_with_seismopt(self):
        """
        This function is only called when no current goal is set.
        """
        self.store_opt_next_file()

        contents = self.parse_current_seismopt_file()

        task = contents["task"]

        # In both cases the next step is to calculate the misfit.
        if task in ("misfit", "misfit_and_gradient"):
            # Make sure all the model files exist.
            model_file_map = {
                contents["prefix"] + "_rho": "x_rho",
                contents["prefix"] + "_vp": "x_vp",
                contents["prefix"] + "_vsh": "x_vsh",
                contents["prefix"] + "_vsv": "x_vsv"}

            # The target, a.k.a model folder.
            target_folder = os.path.join(self.context["optimization_dir"],
                                         contents["model_name"])
            assert not os.path.exists(target_folder), \
                "Folder '%s' already exists." % target_folder

            # Make sure all models files exist.
            for filename in model_file_map.keys():
                filename = os.path.join(contents["folder"], filename)
                assert os.path.exists(filename), "'%s' does not exist." % \
                    filename

            # Good to go. Copy model, set new goal, and next steps and off
            # we go!
            os.makedirs(target_folder)

            for src, target in model_file_map.items():
                src = os.path.join(contents["folder"], src)
                target = os.path.join(target_folder, target)
                shutil.copy2(src, target)

            self.new_goal = "%s %s" % (task, contents["model_name"])
            self.next_steps = [
                {"task_type": "ProjectModel",
                 "inputs": {"regular_model_folder": target_folder},
                 "priority": 0
                },
                # Add a job to plot the starting model at a higher priority.
                {"task_type": "PlotRegularGridModel",
                 "inputs": {"regular_model_folder": target_folder},
                 "priority": 1,
                }
            ]
            return
        elif task == "gradient":
            self.launch_adjoint_source_calculation()
            self.new_goal = "gradient %s" % self.inputs["model_name"]
        else:
            raise NotImplementedError

    def launch_adjoint_source_calculation(self):
        # Make sure the forward run is part of the inputs.
        self._assert_input_exists("hpc_agere_fwd_job_id")
        self._assert_input_exists("model_name")

        self.next_steps = [{
            "task_type": "CalculateAdjointSources",
            "inputs": {
                "model_name": self.inputs["model_name"],
                "hpc_agere_fwd_job_id": self.inputs["hpc_agere_fwd_job_id"]
            },
            "priority": 0
        }]

    def misfit_and_gradient_goal(self, model):
        # This function can be entered at two seperate points in time. Once
        # after the misfit calculation and once after the gradient
        # calculation.

        # We first determine if the misfit has been calculated. That MUST
        # always be the case.
        misfit_file = self.get_misfit_file(model)
        assert os.path.exists(misfit_file), "Misfit must always exist!"


        # Let's figure out if the gradient folder exists.
        gradient_name = model.replace("_model_", "_gradient_")

        gradient_folder = os.path.join(
            self.context["optimization_dir"], gradient_name)

        # If it does not exist, initialize jobs to create it.
        if not os.path.exists(gradient_folder):
            self.launch_adjoint_source_calculation()
            self.new_goal = self.current_goal

        else:
            # Otherwise do the normal thing that is done after a gradient
            # has been calculated.
            self.gradient_goal(model)

    def store_opt_next_file(self):
        """
        Store the current opt.next file to keep track of what is happening.
        """
        now = datetime.datetime.now()
        filename = now.strftime("%y%m%dT%H%M%S_") + "opt.next"

        target = os.path.join(
            self.context["output_folders"]["seismopt_next_files"], filename)

        shutil.copy2(self.seismopt_next, target)

    def misfit_goal(self, model):
        # Initial model. Now we also need the gradient. The first step here
        # is to calculate the adjoint sources.
        if model == "000_1_model":
            # Make sure the forward run is part of the inputs.
            self._assert_input_exists("hpc_agere_fwd_job_id")
            self.next_steps = [{
                "task_type": "CalculateAdjointSources",
                "inputs": {
                    "model_name": model,
                    "hpc_agere_fwd_job_id": self.inputs["hpc_agere_fwd_job_id"]
                },
                "priority": 0
            }]
            self.new_goal = "gradient %s" % model
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
                "inputs": {
                    "hpc_agere_fwd_job_id": self.inputs[
                        "hpc_agere_fwd_job_id"],
                    "model_name": model
                },
                "priority": 0
            }]

            self.new_goal = None

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

            self.new_goal = None
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

            self.new_goal = None

    def get_misfit_file(self, model_name):
        return os.path.join(
            self.context["output_folders"]["misfits"],
            "iteration_%s.txt" % model_name)

    def write_misfit_to_opt(self, iteration, prefix, model_name):
        # Read the misfit.
        misfit_file = self.get_misfit_file(model_name)
        assert os.path.exists(misfit_file)

        with open(misfit_file, "rt") as fh:
            misfit = float(fh.readline())

        output_file = os.path.join(
            self.context["seismopt_dir"], iteration, "misfit_%s" % prefix)
        with open(output_file, "wb") as fh:
            fh.write(struct.pack("d", misfit))

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
        return {
            "new_goal": self.new_goal
        }

    def generate_next_steps(self):
        return self.next_steps
