from job_starter import run_job


run_job("/bin", ["sleep", "10"],
        "stdout.txt", "stderr.txt", "pid.txt")
