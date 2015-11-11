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
        return {"current_goal"}

    def check_pre_staging(self):
        self.current_goal = self.inputs["current_goal"]

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        goal_type, model = self.current_goal.split()
        if goal_type == "misfit":
            self.misfit_goal(model)
        elif goal_type == "gradient":
            self.gradient_goal(model)
        else:
            raise NotImplementedError

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
            raise NotImplementedError

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
            raise NotImplementedError

    def write_misfit_to_opt(self, iteration, prefix, model_name):
        # Read the misfit.
        misfit_file = os.path.join(
            self.context["output_folders"]["misfits"],
            "iteration_%s.txt" % model_name)
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

        assert not expected_contents.difference(actual_contents)

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
