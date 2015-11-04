import datetime
import json
import os
import subprocess
import sys
import time


def create_job(base_folder, job_name, cwd, cmdline):
    os.makedirs(base_folder, exists_ok=True)


class Job():
    def __init__(self, base_folder, name):
        self._base_folder = base_folder
        self.name = name

        job_folder = self.get_job_folder()

        if not os.path.exists(job_folder):
            raise ValueError("Job '%s' not known.")

    def get_job_folder(self):
        return os.path.join(self._base_folder, self.name)


def run_job(cwd, cmdline, stdout_file, stderr_file, pid_file):
    # Adapted from
    # http://code.activestate.com/recipes/
    # 66012-fork-a-daemon-process-on-unix/
    # do the UNIX double-fork magic, see Stevens' "Advanced
    # Programming in the UNIX Environment" for details (ISBN 0201563177)


    def _norm_abs_path(path):
        return os.path.normpath(os.path.abspath(path))

    cwd, stdout_file, stderr_file, pid_file = map(
        _norm_abs_path, [cwd, stdout_file, stderr_file, pid_file])

    try:
        pid = os.fork()
        if pid > 0:
            # exit first parent
            sys.exit(0)
    except OSError as e:
        print("fork #1 failed: %d (%s)" % (e.errno, e.strerror),
              file=sys.stderr)
        sys.exit(1)

    # decouple from parent environment
    os.chdir("/")
    os.setsid()
    os.umask(0)

    # do second fork
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError as e:
        print("fork #2 failed: %d (%s)" % (e.errno, e.strerror),
              file=sys.stderr)
        sys.exit(1)

    starttime = datetime.datetime.now()
    _start = time.time()

    # start the daemon main loop
    with open(stdout_file, "wb") as stdout:
        stdout.write(b"STARTTIME: %b\n" % str(starttime).encode())
        stdout.write(b"---------------------\n")
        stdout.write(b"STDOUT START\n")
        stdout.write(b"---------------------\n")
        with open(stderr_file, "wb") as stderr:
            p = subprocess.Popen(cmdline, cwd=cwd,
                                 stdout=stdout,
                                 stderr=stderr)

    info = {
        "pid": p.pid,
        "cmd_line": cmdline,
        "starttime:": str(starttime)
    }

    pid_file = pid_file
    with open(pid_file, "wt") as fh:
        json.dump(info, fh)

    p.wait()

    endtime = datetime.datetime.now()
    _end = time.time()

    with open(stdout_file, "at") as fh:
        fh.write("\n---------------------\n")
        fh.write("STDOUT END\n")
        fh.write("---------------------\n")
        fh.write("DONE\n")
        fh.write("PID: %i\n" % info["pid"])
        fh.write("RETURNCODE: %i\n" % p.returncode)
        fh.write("RUNTIME: %.3f seconds\n" % (_end - _start))
        fh.write("ENDTIME: %s\n" % endtime)