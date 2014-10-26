#!/usr/bin/env python
#!/usr/bin/env python3
#if you want to use python 2, replace the first line with: #!/usr/bin/env python

import sys
import os
import json

import aimes.skeleton


if __name__ == '__main__':

    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print(('Usage: %s <skeleton_input> <mode> [output_file]' % sys.argv[0]))
        print('        mode should be one of: Shell, Pegasus, Swift, JSON')
        sys.exit(1)

    input_file = sys.argv[1]
    if not os.path.isfile(input_file):
        print("ERROR: input file %s does not exist" % input_file)
        sys.exit(1)
    mode = sys.argv[2]

    if len(sys.argv) == 4:
        outfile = sys.argv[3]
    else:
        outfile = None

    app = aimes.skeleton.Application("test_skeleton", input_file, mode, outfile)
    app.generate()
    app.printTask()
    app.printSetup()
