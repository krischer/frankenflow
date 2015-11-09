from . import task


class Orchestrate(task.Task):
    """
    Orchestrate node always called when its no exactly clear what the next
    step will be.
    """
    # No goal required for orchestration. The whole point of the orchestrate
    # node is to assign a new goal.
    task_requires_active_goal = False

    def check_pre_staging(self):
        raise ValueError("AAHHHHHH")
        pass

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        pass

    def check_post_run(self):
        pass

    def generate_next_steps(self):
        pass
