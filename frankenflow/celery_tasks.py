import datetime
import os
import time

from . import celery
from .tasks import task_map


@celery.task()
def launch_job(job_info, context):
    assert job_info["task_type"] in task_map, "Unknown task."
    job_class = task_map[job_info["task_type"]]

    os.makedirs(job_info["working_dir"], exist_ok=True)
    job = job_class(context=context,
                    inputs=job_info["inputs"],
                    working_dir=job_info["working_dir"],
                    stdout=job_info["stdout"],
                    stderr=job_info["stderr"],
                    logfile=job_info["logfile"])

    report = _launch_job_and_report(job=job)

    return report


def _launch_job_and_report(job):
    report = {}

    stages = [
        ("001_check_pre_staging", "check_pre_staging"),
        ("002_stage_data", "stage_data"),
        ("003_check_post_staging", "check_post_staging"),
        ("004_run", "run"),
        ("005_check_post_run", "check_post_run"),
        ("006_generate_next_steps", "generate_next_steps")
    ]


    for current_stage, method_name in stages:
        info = {}
        report[current_stage]  = info

        _start = time.time()
        info["start_time_stage"] = str(datetime.datetime.now())

        try:
            ret_val = getattr(job, method_name)()
            if current_stage == "006_generate_next_steps":
                report["next_steps"] = ret_val
            _end = time.time()
        except Exception as e:
            _end = time.time()
            info["status"] = "failed"
            info["fail_reason"] = "%s: %s" % (e.__class__.__name__, str(e))
            info["end_time_stage"] = str(datetime.datetime.now())
            info["runtime_stage"] = _end - _start

            report["status"] = "failed"
            report["fail_stage"] = current_stage
            print(report)
            return report

        info["end_time_stage"] = str(datetime.datetime.now())
        info["runtime_stage"] = _end - _start

        job.add_log_entry("Finished stage %s." % current_stage)


    report["status"] = "success"

    return report

