
__author__    = "See AUTHORS file"
__copyright__ = "Copyright 2013-2015, The AIMES Project"
__license__   = "MIT"


"""This API wraps the native skeleton API and represents a instantiated
skeleton as hierarchy of Stages and Tasks.  The API cannot be used to
construct skeletons, only to represent skeletons (the constructors for
Stage and Task are private).

The main class, Skeleton, accepts a file name as construction parameter,
and will use the skeleton module to parse that respective file into a
skeleton represenation, i.e. Stages and Tasks are constructed according
to the skeleton description in the file.  For the mock_api
implementation below, assume that the skeleton module provides
information about the skeleton structure in the priv_<xyz> classes.

Note that Tasks contain lists of inputs and outputs (which are rendered
as dictionaries), and that those lists in turn contain lists of Tasks
(where the inputs originate; where the outputs will be consumed).  That
circularity is ignored in the mockup below (needs weakref's).

*usage of the API*

skeleton = Skeleton('etc/skeleton.conf')

for stage in skeleton.stages:
    print stage.ID
    print stage.tasks

    # Derive stage size
    print len(stage.tasks)

    # Derive stage duration
    print sum(task.runtime for task in stage.tasks)

    # Derive stage staged-in data
    print sum(task.i.size for task in stage.tasks)


for task in skeleton.tasks:
    print task.ID
    print task.description
    print task.stage
    print task.runtime
    print task.inputs
    print task.outputs
    print task.kernel
    print task.executable
    print task.arguments
    print task.cores

    for i in task.inputs:
        print i.ID
        print i.file_name
        print i.file_path
        print i.size
        print i.tasks

    for o in task.outputs:
        print o.ID
        print o.file_name
        print o.file_path
        print o.size
        print o.tasks

"""


import os
import json
import math
import weakref

import skeleton_impl



# -----------------------------------------------------------------------------
#
class Skeleton(object) :
    """This is the base class.  A skeleton is created according to a
    skeleton description (in a file).  It consists of several stages,
    each stage consists of several tasks.  The Skeleton class also
    exposes the complete set of tasks for inspection.

    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, input_file, name=None):

        # derive skeleton name from input_file if not given
        if not name :
            name = os.path.basename (input_file)
            if '.' in name :
                name = '.'.join (name.split ('.')[:-1])

        self.name   = name
        self._impl  = skeleton_impl.Application (name, input_file)
        self._setup = None
        self.stages = list()
        self.tasks  = list()


    # --------------------------------------------------------------------------
    #
    def generate (self, mode) :

        self._impl.generate (mode)

        self._json  = self._impl.as_json  ()
        self._priv  = json.loads (self._json)

      # import pprint
      # pprint.pprint (self.shell)
      # pprint.pprint (self._priv)
      # pprint.pprint (self._setup)
      # sys.exit (0)

        if self.name.endswith (".input"):
            self.name = self.name[:-6]

        self.stages = list()
        for priv_stage in self._priv['stagelist']:
            self.stages.append(Stage(priv_stage, skeleton=self))

        self.tasks = list()
        for stage in self.stages:
            self.tasks += stage.tasks


    # --------------------------------------------------------------------------
    #
    def get_impl (self) :
        return self._impl


    # --------------------------------------------------------------------------
    #
    def get_setup (self) :

        if not self._setup :
            self._setup = self._impl.getSetup ()

        return self._setup[:]


    # --------------------------------------------------------------------------
    #
    def setup (self, verbose=False) :

        if not self._setup :
            self._setup = self._impl.getSetup ()

        for cmd in self._setup :

            if verbose :
                print "running setup command '%s'" % cmd

            if verbose :
                os.system (cmd)

            else:
                import os
                os.system ("/bin/sh -c '%s' 1>/dev/null 2>/dev/null" % cmd)

        return self._setup


    # --------------------------------------------------------------------------
    #
    def __str__ (self) :

        out  = "Skeleton:\n"
        out += "  name  : %s\n" % self.name
        for s in self.stages :
            out += str(s)
        out += "\n"
        return out

    # --------------------------------------------------------------------------
    #
    def get_mode (self, mode) :
    
        if mode not in ['shell', 'swift', 'json', 'pegasus'] : 
            raise ValueError ("ERROR: unknown mode '%s'" % mode)

        if mode == "shell"  : return self._impl.as_shell()
        if mode == "swift"  : return self._impl.as_swift()
        if mode == "json"   : return self._impl.as_json ()
        if mode == "pegasus": return self._impl.as_dax  ()


# -----------------------------------------------------------------------------
#
class Stage(object) :
    """A stage is a set of tasks which are independent from each other
    (vs. tasks from different stages, which can have dependencies).

    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, priv, skeleton):

        self._priv    = priv
        self.name     = self._priv['name']
        self.skeleton = weakref.ref (skeleton)

        self.mode              =     self._priv['mode']
        self.length_para       =     self._priv['length_para']
        self.processes         =     self._priv['processes']
        self.task_type         =     self._priv['task_type']
        self.interleave_option =     self._priv['interleave_option']
        self.input_para        =     self._priv['input_para']
        self.input_dir         =     self._priv['inputdir']
        self.output_para       =     self._priv['output_para']
        self.output_dir        =     self._priv['outputdir']
        self.iter_num          = int(self._priv['iter_num'])
        self.iter_stages       =     self._priv['iter_stages']
        self.iter_sub          =     self._priv['iter_sub']
        self.read_buf          = int(self._priv['read_buf'])
        self.write_buf         = int(self._priv['write_buf'])

        self.tasks = list()

        for priv_task in self._priv['task_list']:
            self.tasks.append(Task(priv_task, stage=self))


    # --------------------------------------------------------------------------
    #
    def __str__ (self) :

        out  = "  Stage : %s\n" % self.name
        out += "    mode              : %s\n" % self.mode
        out += "    length_para       : %s\n" % self.length_para
        out += "    processes         : %s\n" % self.processes
        out += "    task_type         : %s\n" % self.task_type
        out += "    interleave_option : %s\n" % self.interleave_option
        out += "    input_para        : %s\n" % self.input_para
        out += "    input_dir         : %s\n" % self.input_dir
        out += "    output_para       : %s\n" % self.output_para
        out += "    output_dir        : %s\n" % self.output_dir
        out += "    iter_num          : %s\n" % self.iter_num
        out += "    iter_stages       : %s\n" % self.iter_stages
        out += "    iter_sub          : %s\n" % self.iter_sub
        out += "    read_buf          : %s\n" % self.read_buf
        out += "    write_buf         : %s\n" % self.write_buf

        for t in self.tasks :
            out += str(t)
        return out

# -----------------------------------------------------------------------------
#
class Task(object) :
    """A task is an element of a stage, and represents a unit of work.
    A task can depend on multiple input files, and can produce multiple
    output files.

    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, priv, stage):

        self._priv = priv

        self.stage = weakref.ref (stage)

        self.name              =       self._priv['taskid']
        self.task_type         =       self._priv['task_type']
        self.mode              =       self._priv['mode']
        self.command           =       self._priv['command']
        self.args              =       self._priv['args']
        self.length            = float(self._priv['length'])
        self.cores             =   int(self._priv['processes'])
        self.interleave_option =       self._priv['interleave_option']
        self.read_buf          =   int(self._priv['read_buf'])
        self.write_buf         =   int(self._priv['write_buf'])

        self.inputs  = list()
        self.outputs = list()

        # inputs and outputs are represented as dictionaries. One could
        # render those information as classes as well.

        for priv_input in self._priv['inputlist']:
            input_dict = dict()
            input_dict['name'] = priv_input['name']
            input_dict['size'] = priv_input['size']

            self.inputs.append (input_dict)

        for priv_output in self._priv['outputlist']:
            output_dict = dict()
            output_dict['name'] = priv_output['name']
            output_dict['size'] = priv_output['size']

            self.outputs.append (output_dict)


    # --------------------------------------------------------------------------
    #
    def __str__ (self) :

        out  = "    Task: %s\n" % self.name
        out += "        : stage             : %s\n" % self.stage().name
        out += "        : task_type         : %s\n" % self.task_type
        out += "        : mode              : %s\n" % self.mode
        out += "        : command           : %s\n" % self.command
        out += "        : args              : %s\n" % self.args
        out += "        : length            : %s\n" % self.length
        out += "        : cores             : %s\n" % self.cores
        out += "        : interleave_option : %s\n" % self.interleave_option
        out += "        : read_buf          : %s\n" % self.read_buf
        out += "        : write_buf         : %s\n" % self.write_buf

        out += "        : inputs            : %s\n" % len (self.inputs)
        for i in self.inputs :
            out += "        :                   : %10s %s\n" % (i['name'], i['size'])

        out += "        : outputs           : %s\n" % len (self.outputs)
        for o in self.outputs :
            out += "        :                   : %10s %s\n" % (o['name'], o['size'])

        return out

# ------------------------------------------------------------------------------

