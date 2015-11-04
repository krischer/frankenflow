## Broker settings.
BROKER_URL = "redis://localhost:6379"
## Using the database to store task state and results.
CELERY_RESULT_BACKEND = "redis://localhost:6379"

# List of modules to import when celery starts.
CELERY_IMPORTS = (
    "frankenflow.celery_tasks",
)


CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"
CELERY_IGNORE_RESULT = False

CELERY_STATE_DB = "/tmp/celery_state_db"
