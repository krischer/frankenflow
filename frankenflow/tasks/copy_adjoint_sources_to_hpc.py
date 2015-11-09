import glob
import os

from . import task


class CopyAdjointSourcesToHPC(task.Task):
    """
    Copy adjoint sources to the HPC.
    """
    def check_pre_staging(self):
        self._assert_input_exists("model_name")

        # Find the generated adjoint sources and make sure they exist for
        # every event.
        self.events = self.get_events()

        adj_src_folder = os.path.join(self.c["lasif_project"], "OUTPUT",
                                      "adjoint_sources")

        ad_srcs = glob.glob(os.path.join(
            adj_src_folder, "*__ITERATION_%s__*" % self.inputs["model_name"]))

        assert len(ad_srcs) == len(self.events), \
            "%i adjoint source folders found for %i events." % (
                len(ad_srcs), len(self.events))

        # Make sure every folder also has at least on adjoint sources.
        for folder in ad_srcs:
            contents = glob.glob(os.path.join(folder, "ad_src_*"))
            assert contents, "No adjoint sources found in folder %s" % folder

        self.folders_to_copy = ad_srcs

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        self.input_files = os.listdir(self.model_folder)
        for filename in self.input_files:
            src = os.path.join(self.model_folder, filename)
            target = os.path.join(self.remote_target_directory, filename)
            self.sftp_client.put(src, target)

    def check_post_run(self):
        # Make sure all files have been copied.
        remote_files = set(
            self.sftp_client.listdir(self.remote_target_directory))

        local_files = set(self.input_files)

        missing_files = local_files.difference(remote_files)

        assert not missing_files, \
            "The following files could not be copied: %s" % (
                ", ".join(missing_files))

    def generate_next_steps(self):
        next_steps = [
            # Run the forward adjoint.
            {"task_type": "ForwardSimulation",
             "inputs": {
                 "model_name": os.path.basename(self.inputs["model_name"])
             },
             "priority": 0
             }
        ]
        return next_steps
