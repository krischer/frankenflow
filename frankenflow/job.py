import os


class Job():
    def __init__(self, base_folder):
        os.makedirs(base_folder, exists_ok=True)
        self.base_folder = base_folder