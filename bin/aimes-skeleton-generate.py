#!/usr/bin/env python
#!/usr/bin/env python3
#if you want to use python 2, replace the first line with: #!/usr/bin/env python

import sys
import os
import json
import argparse

import aimes.skeleton

# ------------------------------------------------------------------------------
#
def parse_arguments():

    parser = argparse.ArgumentParser()

    parser.add_argument('-i', '--input_file', 
                        dest    = 'input_file', 
                        default = None,
                        help    = "skeleton description file (mandatory)")

    parser.add_argument('-m', '--mode',  
                        dest    = 'mode',  
                        default = 'shell',
                        help    = "output mode: shell, pegasus, swift, json (default: shell)")

    parser.add_argument('-o', '--output_file', 
                        dest    = 'output_file', 
                        default = '-',
                        help    = "output file (default: '-' (stdout))")

    parser.add_argument('-c', '--create-setup', 
                        dest    = 'create_setup', 
                        action  = 'store_true',
                        help    = "create setup script as <output_file>_setup.sh (default: no)")

    parser.add_argument('-r', '--run_setup', 
                        dest    = 'run_setup', 
                        action  = 'store_true',
                        help    = "run setup script (default: no)")

    parser.add_argument('-v', '--verbose', 
                        dest    = 'verbose', 
                        action  = 'store_true',
                        help    = "verbose (default: no)")

    return parser.parse_args(), parser


# ------------------------------------------------------------------------------
#
def main():

    arguments, parser = parse_arguments()

    mode         = arguments.mode
    input_file   = arguments.input_file
    output_file  = arguments.output_file
    create_setup = arguments.create_setup
    run_setup    = arguments.run_setup
    verbose      = arguments.verbose

    if not input_file:
        print("ERROR: input file not specified")
        return 1

    if not os.path.isfile(input_file):
        print("ERROR: input file %s does not exist" % input_file)
        return 1

    if  output_file == '-' :
        output_file  = None

    skeleton = aimes.skeleton.Skeleton (input_file)
    skeleton.generate (mode)

    skeleton_mode  = skeleton.get_mode  (mode)
    skeleton_setup = skeleton.get_setup ()


    if verbose :
        print "Skeleton : "
        print str(skeleton)
        print "Skeleton (setup) : "
        print '\n'.join (skeleton_setup)

    skeleton_setup = skeleton.get_setup ()

    if output_file :

        output_mode = mode.lower()
        if  output_mode == 'pegasus':
            output_mode =  'dax'

        with open ("%s.%s" % (output_file, output_mode), "w") as out :
            print "writing %s.%s" % (output_file, output_mode)
            out.write ("\n%s\n\n" % skeleton_mode)


        if create_setup :

            output_dir  = os.path.dirname  (output_file)
            output_base = os.path.basename (output_file)

            if '.' in output_base :
                output_base = '.'.join (output_base.split ('.')[:-1])

            setup_output_file = ""
            if output_dir :
                setup_output_file += "%s/" % output_dir
            setup_output_file += "%s_setup.sh" % output_base

            with open (setup_output_file, 'w') as out :
                print "writing %s" % (setup_output_file)
                out.write ("#!/bin/sh\n\n%s\n\n" % '\n'.join (skeleton_setup))

    else :
        print "\nSkeleton (%s) :\n\n%s\n" % (mode, skeleton_mode)

        if create_setup :
            print "Setup (shell):\n\n%s\n\n" % '\n'.join (skeleton_setup)


    if run_setup :
        skeleton.setup ()


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":
    sys.exit(main())


