import glob
import os

from . import task


class CopyAdjointSourcesToHPC(task.Task):
    """
    Copy adjoint sources to the HPC.
    """
    def check_pre_staging(self):
        self._assert_input_exists("model_name")
        self._assert_input_exists("hpc_agere_run_name")

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

        # Folder of the adjoint source on the remote HPC host.
        self.remote_adjoint_source_directory = os.path.join(
            self.c["hpc_adjoint_source_folder"], self.inputs["model_name"])

        contents = self.sftp_client.listdir(
            self.c["hpc_adjoint_source_folder"])
        assert self.inputs["model_name"] not in contents, \
            "Remote folder '%s' already exists." % (
                self.remote_adjoint_source_directory)

    def stage_data(self):
        self.sftp_client.mkdir(self.remote_adjoint_source_directory)

    def check_post_staging(self):
        # Will make sure it exists.
        self.sftp_client.listdir(self.remote_adjoint_source_directory)

    def run(self):
        for folder in self.folders_to_copy:
            target = os.path.join(
                self.remote_adjoint_source_directory,
                os.path.basename(folder))

            cmd = ["rsync", "-aP", folder + "/", "%s:%s" % (
                self.c["hpc_remote_host"], target)]
            print(cmd)

    def check_post_run(self):
        # Make sure everything has been copied.
        for folder in self.folder_to_copy:
            local_contents = set(os.listdir(folder))
            remote_folder = os.path.join(
                self.remote_adjoint_source_directory,
                os.path.basename(folder))
            remote_contents = set(self.sftp_client.listdir(remote_folder))

            difference = local_contents.difference(remote_contents)

            assert not difference, (
                "Remote folder '%s' has different contents than local folder "
                "'%s'." % (remote_folder, folder))

    def generate_next_steps(self):
        next_steps = [
            # Run the forward adjoint.
            {"task_type": "AdjointSimulation",
             "inputs": {
                 "model_name": self.inputs["model_name"],
                 "adjoint_source_directory":
                     self.remote_adjoint_source_directory
             },
             "priority": 0
             }
        ]
        return next_steps
