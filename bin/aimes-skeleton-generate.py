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

    parser.add_argument('-v', '--verbose', 
                        dest    = 'verbose', 
                        action  = 'store_true',
                        help    = "verbose (default: off)")

    return parser.parse_args(), parser


# ------------------------------------------------------------------------------
#
def main():

    arguments, parser = parse_arguments()

    mode        = arguments.mode
    input_file  = arguments.input_file
    output_file = arguments.output_file
    verbose     = arguments.verbose

    if not input_file:
        print("ERROR: input file not specified")
        return 1

    if not os.path.isfile(input_file):
        print("ERROR: input file %s does not exist" % input_file)
        return 1

    if  output_file == '-' :
        output_file = None

    skeleton = aimes.skeleton.Skeleton ("test_skeleton", input_file)
    skeleton.generate(mode, output_file)

    if verbose :
        print "Skeleton : "
        print str(skeleton)
        print "Skeleton (setup) : "
        print '\n'.join (skeleton.get_setup ())


    skeleton_str = skeleton.convert (mode)

    if output_file :
        with open (output_file, "w") as out :
            out.write ("\n%s\n\n" % skeleton_str)

    else :
        print "\n%s\n\n" % skeleton_str


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":
    sys.exit(main())

