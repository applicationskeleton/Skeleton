######################################
AIMES Skeleton |version| Documentation
######################################

The Application Skeleton (just `Skeleton' hereafter) tool lets users quickly and
easily produce a synthetic distributed application that runs in a distributed
environment, e.g. grids, clusters, clouds. 

A skeleton application is a synthetic application that is intended to represent
the computation, I/O, and networking behavior of a real application.  Such
a skeleton application is composed of one or more stages.  The user needs to
define each stage's task type, number of processes, task length, computation and
I/O interleaving option, read/write buffer size, input source, input size,
output size, and other properties.

The Skeleton tool reads an application description file as input, and produces
three groups of outputs: \begin{itemize}

* **Preparation Scripts:** The preparation scripts are run to produce the
  input/output directories and input files for the synthetic applications.

* **Executables:** Executables are the tasks of each application stage. (We
  assume different stages use different executables.)

* **Application:** The overall application can be expressed in four different
  formats: shell commands, JSON description, a Pegasus DAG, and a Swift script.

The shell commands can be executed in sequential order on a single machine.

The Pegasus DAG and the Swift script can be executed on a local machine or in
a distributed environment.  Executing the Pegasus DAG or Swift script requires
Pegasus or Swift, respectively.

The JSON description aims to support interoperation with other python modules,
specifically with other components of the AIMES project.


A skeleton application is also acceissible programatically, i.e. the Skeleton
API supports access to all skeleton application details.


**Get involved or contact us:**

+-------+-----------------------------------+--------------------------------------------------+
| |Git| | **AIMES Skeletons on GitHub:**    | https://github.com/applicationskeleton/Skeleton/ |
+-------+-----------------------------------+--------------------------------------------------+
| |Goo| | **AIMES Skeletons Mailing List:** |                                                  |
+-------+-----------------------------------+--------------------------------------------------+

.. |Git| image:: images/github.jpg
.. |Goo| image:: images/google.png


#########
Contents:
#########

.. toctree::
   :numbered:
   :maxdepth: 2

   usage.rst
   api.rst


##################
Indices and tables
##################

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

