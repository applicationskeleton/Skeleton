

from skeleton_api import *

import os

_mod_root = os.path.dirname (__file__)

version        = open (_mod_root + "/VERSION",     "r").readline ().strip ()
version_detail = open (_mod_root + "/VERSION.git", "r").readline ().strip ()

# where to find task sourece code, and how to compile it
TASK_LOCATION  = "%s/task.c" % os.path.dirname (__file__)
TASK_COMPILE   = "cc -o task -lm task.c"

