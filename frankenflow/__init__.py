from celery import Celery

celery = Celery(main="frankenflow")
celery.config_from_object("frankenflow.celeryconfig")

from celery.signals import after_task_publish


@after_task_publish.connect
def update_sent_state(sender=None, body=None, **kwargs):
    """
    Change state to SENT if done.
    """
    # the task may not exist if sent using `send_task` which
    # sends tasks by name, so fall back to the default result backend
    # if that is the case.
    task = celery.tasks.get(sender)
    backend = task.backend if task else celery.backend

    backend.store_result(body['id'], None, "SENT")