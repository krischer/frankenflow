import abc
import datetime
import glob
import os
import shutil
import subprocess
import time

import paramiko


class TaskCheckFailed(Exception):
    pass


class TaskFailedPreStagingCheck(Exception):
    pass


class Task(metaclass=abc.ABCMeta):
    """
    A single task.

    It has 6 stages:

    1. check_pre_staging: Check if the task can be run in the first place, i.e.
       check if everything necessary is available.
    2. stage_data: Stage the data for the run. Only do quick things
       here...for long data copies and moves use a separate task.
    3. check_post_staging: Check if the staging was successful.
    4. run: Run the task.
    5. check_post_run: Check if the task has been successful.
    6. generate_next_steps: Generate the next task.

    If any stage raises, the task will not complete successfully.
    """
    # By default all tasks require an active goal. Any task that does not
    # require an active goal will not be assigned one.
    task_requires_active_goal = True

    def __init__(self, context, inputs, working_dir, stdout, stderr, logfile):
        self.context = context
        # Shortcut because its required to have all over the place.
        self.c = self.context["config"]

        self.inputs = inputs
        self.working_dir = working_dir
        self.stdout = stdout
        self.stderr = stderr
        self.logfile = logfile

    @abc.abstractproperty
    def required_inputs(self):
        """
        A set of keys that must be available as inputs.
        """
        pass

    @abc.abstractmethod
    def check_pre_staging(self):
        pass

    @abc.abstractmethod
    def stage_data(self):
        pass

    @abc.abstractmethod
    def check_post_staging(self):
        pass

    @abc.abstractmethod
    def run(self):
        pass

    @abc.abstractmethod
    def check_post_run(self):
        pass

    @abc.abstractmethod
    def generate_next_steps(self):
        pass

    def get_events(self):
        events = glob.glob(os.path.join(self.c["lasif_project"], "EVENTS",
                                        "*.xml"))
        events = [os.path.splitext(os.path.basename(_i))[0]
                  for _i in events]

        assert len(events) == self.c["number_of_events"], \
            "LASIF projects has %i events. Only %i specified in confg." % (
                len(events), self.c["number_of_events"])

        return events

    def _assert_input_exists(self, input):
        assert input in self.inputs, "'%s' must be part of the inputs" % (
            input)

    def add_log_entry(self, msg):
        with open(self.logfile, "at") as fh:
            fh.write("[%s] %s\n" % (str(datetime.datetime.now()), msg))

    def _init_ssh_and_stfp_clients(self):
        # Load the config.
        user_config_file = os.path.expanduser("~/.ssh/config")
        ssh_config = paramiko.SSHConfig()
        with open(user_config_file) as fh:
            ssh_config.parse(fh)

        # Use it to get host and user.
        info = ssh_config.lookup(self.context["config"]["hpc_remote_host"])
        # For some reason paramiko does not make it directly pluggable
        # into the connect() method...
        info["username"] = info["user"]
        del info["user"]

        self.ssh_client = paramiko.SSHClient()
        # Should be safe enough in our controlled environment.
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh_client.load_system_host_keys()
        self.ssh_client.connect(**info)

        self.sftp_client = self.ssh_client.open_sftp()

        self.add_log_entry(
            "Successfully initialized SSH and SFTP connection to %s@%s" % (
                info["username"], info["hostname"]))

    def __del__(self):
        # Close ssh and sftp clients in the destructor.
        if hasattr(self, "ssh_client"):
            self.ssh_client.close()
            self.sftp_client.close()

    def _run_ssh_command(self, cmd):
        self.add_log_entry("Executing command over SSH: '%s'" % cmd)
        _, stdout, stderr = self.ssh_client.exec_command(cmd)
        # Force synchronous execution.
        stdout = stdout.readlines()
        stderr = stderr.readlines()
        return stdout, stderr

    def _run_external_script(self, cwd, cmd):
        starttime = datetime.datetime.now()
        _start = time.time()

        self.add_log_entry("Locally executing cmd '%s' in folder '%s' ..." % (
            cmd, cwd))

        # start the daemon main loop
        with open(self.stdout, "ab") as stdout:
            stdout.write(b"\n\n\n\n\n================================\n")
            stdout.write(b"\n================================\n")
            stdout.write(b"STARTTIME: %b\n" % str(starttime).encode())
            stdout.write(b"---------------------\n")
            stdout.write(b"STDOUT START\n")
            stdout.write(b"---------------------\n")
            with open(self.stderr, "ab") as stderr:
                p = subprocess.Popen(cmd, cwd=cwd,
                                     stdout=stdout,
                                     stderr=stderr)

        p.wait()

        endtime = datetime.datetime.now()
        _end = time.time()

        with open(self.stdout, "at") as fh:
            fh.write("\n---------------------\n")
            fh.write("STDOUT END\n")
            fh.write("---------------------\n")
            fh.write("DONE\n")
            fh.write("PID: %i\n" % p.pid)
            fh.write("RETURNCODE: %i\n" % p.returncode)
            fh.write("CMD: %s\n" % (str(cmd)))
            fh.write("RUNTIME: %.3f seconds\n" % (_end - _start))
            fh.write("ENDTIME: %s\n" % endtime)

        return p.returncode

    def copy_blockfiles(self, target_dir):
        """
        The blockfiles constantly have to copied. Thus this is a class method.

        :param target_dir: The target directory.
        """
        for file in self.context["data"]["block_files"]:
            filename = os.path.basename(file)
            shutil.copy2(file, os.path.join(target_dir, filename))
