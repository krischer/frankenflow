import abc
import datetime
import os
import shutil
import subprocess
import time


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
    3. check_post_data_stage: Check if the staging was successful.
    4. run: Run the task.
    5. check_post_run: Check if the task has been successful.
    6. stage_data_out: Copy the output data where it needs to go and
        clean-up everything.
    7. finalize: Final check. Has to return True for the task to be
       considered done!

    The checking stages have to return True, otherwise it will abort!
    """
    def __init__(self, context, inputs, working_dir, stdout, stderr):
        self.context = context
        self.inputs = inputs
        self.working_dir = working_dir
        self.stdout = stdout
        self.stderr = stderr

    def _run_external_script(self, cwd, cmd):
        starttime = datetime.datetime.now()
        _start = time.time()

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

    def copy_blockfiles(self, target_dir):
        """
        The blockfiles constantly have to copied. Thus this is a class method.

        :param target_dir: The target directory.
        """
        for file in self.context["data"]["block_files"]:
            filename = os.path.basename(file)
            shutil.copy2(file, os.path.join(target_dir, filename))

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
