import os

from . import task


class TaperAndPreconditionGradient(task.Task):
    """
    Tapers and preconditions a gradient.
    """
    @property
    def required_inputs(self):
        return {"iteration_name"}

    def check_pre_staging(self):
        self.hdf5_gradient_filename = \
            self.get_gradient_file(self.inputs["iteration_name"])

        self.depth_scaling_file = os.path.join(
            self.context["data_folder"], "depth_scaling.json")

        if not os.path.exists(self.depth_scaling_file):
            msg = ("Depth scaling files '%s' does not exists. Please run "
                   "`agere determine_depth_scaling` with the current gradient "
                   "and place the resulting JSON file in the correct folder.")
            raise Exception(msg % self.depth_scaling_file)

        assert os.path.exists(self.hdf5_gradient_filename), \
            "HDF5 gradient must exist!"

        self.hdf5_output_gradient_filename = \
            self.get_gradient_file(self.inputs["iteration_name"] +
                                   "_preconditioned")
        assert not os.path.exists(self.hdf5_output_gradient_filename), \
            "Output HDF5 gradient must not exist!"

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        cmd = [self.context["config"]["agere_cmd"],
               "taper_and_precondition_gradient",
               self.hdf5_gradient_filename,
               self.hdf5_output_gradient_filename,
               self.depth_scaling_file,
               "--longitude_offset_in_km=%s" % str(
               self.c["taper_longitude_offset_in_km"]),
               "--colatitude_offset_in_km=%s" % str(
                   self.c["taper_colatitude_offset_in_km"]),
               "--depth_offset_in_km=%s" % str(
                   self.c["taper_depth_offset_in_km"]),
               "--longitude_width_in_km=%s" % str(
                   self.c["taper_longitude_width_in_km"]),
               "--colatitude_width_in_km=%s" % str(
                   self.c["taper_colatitude_width_in_km"]),
               "--depth_width_in_km=%s" % str(
                   self.c["taper_depth_width_in_km"])]

        returncode = self._run_external_script(cwd=".", cmd=cmd)
        assert returncode == 0, \
            "Gradient preconditioning exited with return code %i." % returncode

    def check_post_run(self):
        assert os.path.exists(self.hdf5_output_gradient_filename), \
            "The preconditioned and tapered gradient file did not get created."

    def generate_next_steps(self):
        next_steps = [
            # Plot the processed gradient.
            {"task_type": "PlotHDF5Gradient",
             "inputs": {"tag": "_preconditioned"},
             "priority": 1
             },
            # Orchestrate.
            {"task_type": "Orchestrate",
             "priority": 0
             }
        ]
        return next_steps
