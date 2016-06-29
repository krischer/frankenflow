import os
import time

from . import task
from .. import push_notifications


class ForwardSimulation(task.Task):
    """
    Task copying a model on the spectral element grid to the HPC running the
    simulation.

    The model must reside in the associated LASIF project.
    """
    @property
    def required_inputs(self):
        return {"iteration_name"}

    def check_pre_staging(self):
        self._init_ssh_and_stfp_clients()

        # Remote input file directory.
        self.remote_input_file_directory = \
            self.context["config"]["hpc_remote_input_files_directory"]

        # Make sure the directory exists and the amount of input files is
        # equal to number of events defined in the config file.
        try:
            input_files = self.remote_listdir(
                self.remote_input_file_directory)
        except FileNotFoundError:
            raise FileNotFoundError("Remote input file directory does not "
                                    "exists. Are you sure it is correct?")
        assert len(input_files) == \
           self.context["config"]["number_of_events"], (
            "The remote input files directory '%s' must have %i folders." % (
                self.remote_input_file_directory,
                self.context["config"]["number_of_events"]))

        # Model directory.
        self.remote_model_directory = os.path.join(
            self.context["config"]["hpc_agere_project"],
            "__MODELS")

        # Make sure the model directory exists.
        existing_models = self.remote_listdir(self.remote_model_directory)
        assert self.model_name in existing_models, (
            "Model '%s' does not exist on the HPC" % self.model_name)

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        c = self.context["config"]

        # Make sure the model directory exists.
        cmd = ("{agere} run_forward --model={model_name} --fw-lpd={fw_lpd} "
               "--wall-time-per-event={walltime_per_event} "
               "--pml-count={pml_count} "
               "--parallel-events={parallel_events} {input_files}").format(
            agere=self.c["hpc_agere_cmd"],
            model_name=self.model_name,
            fw_lpd=c["forward_wavefield_storage_degree"],
            walltime_per_event=c["walltime_per_event_forward"],
            parallel_events=c["parallel_events"],
            pml_count=c["pml_count"],
            input_files=os.path.join(self.remote_input_file_directory, "*"))

        stdout, stderr = self._run_ssh_command(cmd)

        with open(self.stdout, "at") as fh:
            fh.write("\n\n")
            fh.write("".join(stdout))
            fh.write("\n\n")

        with open(self.stderr, "at") as fh:
            fh.write("\n\n")
            fh.write("".join(stderr))
            fh.write("\n\n")

        # Parse stdout to figure out the agere job number.
        for line in stdout:
            line = line.strip()
            if "Initializing run" in line:
                self.hpc_agere_fwd_job_id = line.split("'")[1]
                break
        else:
            raise ValueError("Could not find run number on stdout: %s" %
                             stdout)

        # Send a push notification.
        push_notifications.send_notification(
            title="Launched Forward Simulation!",
            message="Forward simulation for iteration %s" %
                self.iteration_name)

        # Check if job is done all two minutes.
        while True:
            time.sleep(10)

            stdout, stderr = self._run_ssh_command(
                "%s status" % self.c["hpc_agere_cmd"])

            finished = False

            for line in stdout:
                line = line.strip()
                if line.startswith("JOB NUMBER") or line.startswith("====="):
                    continue
                line = line.split()
                if line[0] != self.hpc_agere_fwd_job_id:
                    continue

                status = line[1].upper()

                self.add_log_entry("Current status of remote job: %s" % status)

                if status == "FINISHED":
                    finished = True
                break
            else:
                raise ValueError("`agere_status` did not contain run '%s'" %
                                 self.hpc_agere_fwd_job_id)

            if not finished:
                continue
            else:
                # Send a push notification.
                push_notifications.send_notification(
                    title="Finished Forward Simulation!",
                    message="Done with forward simulation for model %s" %
                            self.model_name)
                break

    def check_post_run(self):
        c = self.context["config"]

        # Check if some waveform folders have been generated. This is necessary
        # as my current SES3D version sometimes chooses not to simulate all
        # events.
        self.output_directory = os.path.join(
            c["hpc_agere_project"], "__WAVEFORMS", self.hpc_agere_fwd_job_id)

        event_folders = self.remote_listdir(self.output_directory)
        assert len(event_folders), "Run should have resulted in some events."

    def generate_next_steps(self):
        next_steps = [
            # Tar the waveforms.
            {"task_type": "TarWaveformsOnHPC",
             "inputs": {
                 "hpc_agere_fwd_job_id": self.hpc_agere_fwd_job_id
             },
             "priority": 0
             }
        ]
        return next_steps
