import os


class JobManager():
    def __init__(self, base_folder):
        os.makedirs(base_folder, exist_ok=True)
        self.base_folder = base_folder

        self.__init_job_db()

    def __init_job_db(self):
        pass
