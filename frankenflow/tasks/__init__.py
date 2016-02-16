import importlib
import os
import re

for module in os.listdir(os.path.dirname(__file__)):
    if module == '__init__.py' or not module.endswith('.py'):
        continue

    module = ".tasks." + re.sub("\.py$", "", module)
    importlib.import_module(module, package="frankenflow")
del module


from .task import Task

# All available tasks implemented as subclasses of the Task class.
task_map = {_i.__name__: _i for _i in Task.__subclasses__()}