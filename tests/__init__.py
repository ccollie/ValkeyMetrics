import os.path
from sys import platform

from test_query_range import RDB_PATH

def get_platform():
    return platform.lower()

def get_dynamic_lib_extension():
    system = get_platform()

    if system == "windows":
        return ".dll"
    elif system == "darwin":
        return ".dylib"
    elif system == "linux":
        return ".so"
    else:
        raise Exception(f"Unsupported platform: {system}")

PLATFORM = get_platform()
MODULE_PATH = os.path.abspath("../target/debug/libvalkey_metrics{}".format(get_dynamic_lib_extension()))
LOG_DIR = "./logs"
RDB_PATH = os.path.abspath(RDB_PATH)