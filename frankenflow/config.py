import json
import os


class Config():
    def __init__(self, filename):
        self.__filename = filename

        if not os.path.exists(filename):
            self.config = {
            }
            self.serialize()
        else:
            self.deserialize()

        self.assert_config()

    def assert_config(self):
        self._assert_config_file_exists("agere_cmd")
        self._assert_config_file_exists("lasif_cmd")

        self._assert_config_folder_exists("lasif_project")

        # Variables to deal with the HPC remote host.
        self._assert_var_exists("hpc_remote_host")
        self._assert_var_exists("hpc_agere_project")
        self._assert_var_exists("hpc_remote_input_files_directory")
        self._assert_var_exists("hpc_agere_cmd")
        # Folder to store the adjoint sources on the hpc.
        self._assert_var_exists("hpc_adjoint_source_folder")

        self._assert_var_exists("number_of_events", var_type=int)
        self._assert_var_exists("forward_wavefield_storage_degree",
                                var_type=int)
        self._assert_var_exists("parallel_events", var_type=int)
        self._assert_var_exists("pml_count", var_type=int)
        self._assert_var_exists("walltime_per_event_forward", var_type=float)
        self._assert_var_exists("walltime_per_event_adjoint", var_type=float)

        # Smoothing parameters.
        self._assert_var_exists("smoothing_theta", var_type=float)
        self._assert_var_exists("smoothing_phi", var_type=float)
        self._assert_var_exists("smoothing_r", var_type=float)


    def _assert_var_exists(self, key, var_type=None):
        assert key in self.config, \
            "'%s' must be given in the config file." % key
        if var_type is not None:
            assert isinstance(self.config[key], var_type), \
                "Config variable '%s' not of type '%s'." % (key,
                                                            var_type.__name__)

    def _assert_config_file_exists(self, key):
        self._assert_var_exists(key)

        filename = self.config[key]
        assert os.path.isfile(filename), \
            "File '%s' for config value '%s' must exist." % (filename, key)

    def _assert_config_folder_exists(self, key):
        self._assert_var_exists(key)

        filename = self.config[key]
        assert os.path.isdir(filename), \
            "Folder '%s' for config value '%s' must exist." % (filename, key)

    def serialize(self):
        with open(self.__filename, "wt") as fh:
            json.dump(self.config, fh)

    def deserialize(self):
        with open(self.__filename, "rt") as fh:
            self.config = json.load(fh)
        self.assert_config()

    def __getitem__(self, item):
        return self.config[item]