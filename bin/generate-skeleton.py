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
    parser.add_argument('-m', '--mode',  dest='mode',  default='shell',
            help="output mode: shell, pegasys, swift, json (default: shell)")
    parser.add_argument('-i', '--input_file', dest='input_file', default=None,
            help="skeleton description file (mandatory)")
    parser.add_argument('-o', '--output_file', dest='output_file', default='-',
            help="output file (default: '-' (stdout))")

    return parser.parse_args(), parser


# ------------------------------------------------------------------------------
#
def main():

    arguments, parser = parse_arguments()

    mode        = arguments.mode
    input_file  = arguments.input_file
    output_file = arguments.output_file

    if not os.path.isfile(input_file):
        print("ERROR: input file %s does not exist" % input_file)
        return 1

    if  output_file == '-' :
        output_file = None

    app = aimes.skeleton.Application("test_skeleton", input_file, mode, output_file)
    app.generate()
    app.printTask()
    app.printSetup()


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":
    sys.exit(main())

