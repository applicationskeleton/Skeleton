

from skeleton_api import *

import os

_mod_root = os.path.dirname (__file__)

version        = open (_mod_root + "/VERSION",     "r").readline ().strip ()
version_detail = open (_mod_root + "/VERSION.git", "r").readline ().strip ()


