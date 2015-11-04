import os
import sys
import traceback


def assert_files_exist(folder, file_names):
    """
    Helper function that checks that all files are in the folder.

    Will check they exist and that they are files and have some content.
    """
    for name in file_names:
        filename = os.path.join(folder, name)
        assert os.path.exists(filename), "File '%s' does not exist" % filename
        assert os.path.isfile(filename), "'%s' is not a file" % filename
        assert os.path.getsize(filename), "'%s' is an empty file" % filename


def assert_files_dont_exist(folder, file_names):
    """
    Helper function that checks that some files don't exist.
    """
    for name in file_names:
        filename = os.path.join(folder, name)
        assert not os.path.exists(filename), (
            "File '%s' already exists." % filename)


def collect_traceback(traceback_limit):
        # Extract traceback from the exception.
        exc_info = sys.exc_info()
        stack = traceback.extract_stack(
            limit=traceback_limit)
        tb = traceback.extract_tb(exc_info[2])
        full_tb = stack[:-1] + tb
        exc_line = traceback.format_exception_only(
            *exc_info[:2])
        tb = ("Traceback (At max %i levels - most recent call "
              "last):\n" % traceback_limit)
        tb += "".join(traceback.format_list(full_tb))
        tb += "\n"
        tb += "".join(exc_line)

        return tb
