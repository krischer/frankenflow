import os
import time

from . import task


class ForwardSimulation(task.Task):
    """
    Task copying a model on the spectral element grid to the HPC running the
    simulation.

    The model must reside in the associated LASIF project.
    """
    def check_pre_staging(self):
        self._init_ssh_and_stfp_clients()

        assert "model_name" in self.inputs, "'model_name' must be part of " \
                                            "the inputs"

        self.inputs["model_name"] = self.inputs["model_name"].lower()

        # Remote input file directory.
        self.remote_input_file_directory = \
            self.context["config"]["hpc_remote_input_files_directory"]

        # Make sure the directory exists and the amount of input files is
        # equal to number of events defined in the config file.
        try:
            input_files = self.sftp_client.listdir(
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
        existing_models = self.sftp_client.listdir(self.remote_model_directory)
        assert self.inputs["model_name"] in existing_models, (
            "Model '%s' does not exist on the HPC" % (
                self.inputs["model_name"]))

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        c = self.context["config"]

        # Make sure the model directory exists.
        cmd = ("agere run_forward --model={model_name} --fw-lpd={fw_lpd} "
               "--wall-time-per-event={walltime_per_event} "
               "--pml-count={pml_count} "
               "--parallel-events={parallel_events} {input_files}").format(
            model_name=self.inputs["model_name"],
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
                self.job_number = line.split("'")[1]
                break
        else:
            raise ValueError("Could not find run number on stdout")

        # Check if job is done all five minutes.
        while True:
            time.sleep(20)

            stdout, stderr = self._run_ssh_command("agere status")

            finished = False

            for line in stdout:
                line = line.strip()
                if line.startswith("JOB NUMBER") or line.startswith("====="):
                    continue
                line = line.split()
                if line[0] != self.job_number:
                    continue

                status = line[1].upper()

                self.add_log_entry("Current status of remote job: %s" % status)

                if status == "FINISHED":
                    finished = True
                break
            else:
                raise ValueError("`agere_status` did not contain run '%s'" %
                                 self.job_number)

            if not finished:
                continue
            else:
                break

    def check_post_run(self):
        c = self.context["config"]
        # Check if all waveform folders have been generated. This is necessary
        # as my current SES3D version sometimes chooses not to simulate all
        # events.
        self.output_directory = os.path.join(
            c["hpc_agere_project"], "__WAVEFORMS", self.job_number)

        event_folders = self.sftp_client.listdir(self.output_directory)
        assert len(event_folders) == c["number_of_events"], \
            "Run should have resulted in '%s' events." % c["number_of_events"]

    def generate_next_steps(self):
        next_steps = [
            # Tar the waveforms.
            {"task_type": "TarWaveformsOnHPC",
             "inputs": {
                 "job_number": self.job_number,
                 # Keep track of the current model and pass it from task to
                 # task.
                 "model_name": self.inputs["model_name"]
             },
             "priority": 0
             }
        ]
        return next_steps
