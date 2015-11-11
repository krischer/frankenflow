import json
import os


class FlowStatus(object):
    """
    Simple persistent status.

    Like a dictionary just always stored on disc.
    """
    def __init__(self, filename):
        self._filename = filename
        self._deserialize()

    def __getitem__(self, item):
        if item not in self.__status:
            return None
        self._deserialize()
        return self.__status[item]

    def __setitem__(self, key, value):
        self.__status[key] = value
        self._serialize()

    def _deserialize(self):
        if not os.path.exists(self._filename):
            self.__status = {}
        else:
            with open(self._filename, "rt") as fh:
                self.__status = json.load(fh)
        return self.__status

    def _serialize(self):
        with open(self._filename, "wt") as fh:
            json.dump(self.__status, fh)