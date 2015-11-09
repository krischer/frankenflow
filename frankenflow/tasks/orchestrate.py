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
        self._assert_input_exists("current_goal")
        self.current_goal = self.inputs["current_goal"]

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        goal_type, model = self.current_goal.split()
        if goal_type == "misfit":
            self.misfit_goal(model)
        elif goal_type == "gradient":
            self.gradient_goal(model)
        else:
            raise NotImplementedError

    def misfit_goal(self, model):
        # Initial model. Now we also need the gradient. The first step here
        # is to calculate the adjoint sources.
        if model == "000_1_model":
            self.next_steps = [{
                "task_type": "CalculateAdjointSources",
                "inputs": {
                    "model_name": model
                },
                "priority": 0
            }]
            self.new_goal = "gradient %s" % model
        else:
            raise NotImplementedError

    def gradient_goal(self, model):
        raise NotImplementedError

    def check_post_run(self):
        return {
            "new_goal": self.new_goal
        }

    def generate_next_steps(self):
        return self.next_steps
