#!/usr/bin/env python

import os
import sys
import stat
import time

import radical.utils   as ru
import radical.synapse as rs

# ------------------------------------------------------------------------------
#
MPI = False

if mpi:
    from mpi4py import MPI as mpi
else:
    mpi = None


# ------------------------------------------------------------------------------
#
charset = "0123456789" + \
          "abcdefghijklmnopqrstuvwxyz" + \
          "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
charlen = len(charset)

def rand_str(length):

    ret = ""
    for i in range(length):
        ret += charset[rand(charlen)]
    return ret


# ------------------------------------------------------------------------------
#
def prep_output(fname):

    dname = os.path.dirname(fname)

    try:
        os.path.mkdirs(dname)
    except os.error:
        # dir exists
        pass

    os.chmod (dname, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)


# ------------------------------------------------------------------------------
#
def readfiles(input_files, bufsize):

    for input_file in input_files

        count      = 0;
        total_size = 0;

        fd    = os.open(input_file, 'r')
        fsize = os.stat(input_file).st_size

        print "reading input file: %s fsize: %ld num_reads: %d" % \
                (input_file, fsize, fsize/bufsize)

        while total_size < fsize:

            buffer = read(fd, min(bufsize, fsize - total_size))

            if len(buffer) < 0:
                raise RuntimeError("cannot read from %s" % input_file)

            # print "read: %s" % buffer
            total_size += len(buffer)
            count      += 1

            if not count % 1000:
                print "reading operations: %d total_size: %d st_size: %d" % \
                       (count, total_size, fsize)

        print "read : %d bytes to %s with bufsize: %d" % \
                (total_size, input_file, bufsize)

        os.close(fd)


# ------------------------------------------------------------------------------
#
def writefiles(output_files, output_sizes, bufsize):

    buffer = ""

    for output_file, output_size in zip(output_files, output_sizes):

        fd = None
        size;
        count = 0;
        total_size = 0;

        prep_output(output_files)

        fd = os.open(output_file, 'w+')

        while total_size < output_size:

            buffer = rand_str(bufsize)
            size = os.write(fd, buffer, min(bufsize, output_sizes[i] - total_size))

            if size < 0:
                raise RuntimeError("cannot write to %s" % output_files)

            os.fsync(fd)
            total_size += size;
            count      += 1

            if not count % 1000:
                print "write operations: %d total_size: %d output_size: %d" % \ 
                       (count, total_size, output_size)

        print "write: %d bytes to %s with bufsize: %d" % \
                (total_size, output_files[i], bufsize)

        os.close(fd)


# ------------------------------------------------------------------------------
#
def compute(task_length):

    print "sleep interval: %f" % task_length
    time.sleep(task_length)


# ------------------------------------------------------------------------------
#
# Run floating point calculations as a token of work
# compute_flop with run task_length number of floating point
# operations
# 
def compute_flop(task_length):
    
    pass


# ------------------------------------------------------------------------------
#
def read_compute(input_files, bufsize, task_length):

    pass


# ------------------------------------------------------------------------------
#
def compute_write (task_length, output_files, output_sizes, bufsize):

    pass


# ------------------------------------------------------------------------------
#
def read_compute_write(input_files, r_bufsize, task_length, 
        output_files, output_sizes, w_bufsize):

    pass


# ------------------------------------------------------------------------------
#
def main(argv):

    argc   = len(argv)
    master = False

    if mpi:

        print "parallel: %d" % argc

        comm = mpi.COMM_WORLD
        rank = comm.Get_rank()
        size = comm.Get_size()

        if rank == 0:
            # FIXME: why is only rank 0 ever doing anything?  That will miss I/O
            #        contention, for example...
            master = True

    else:
        print "serial: %d" % argc
        master = True


    if ( argc < 9 )
        raise ValueError("insufficient arguments")

    # input parameter processing
    type_s         = str(argv[1]
    num_proc       = int(argv[2])
    task_length    = float(argv[3])
    read_buf       = int(argv[4])
    write_buf      = int(argv[5])
    num_input      = int(argv[6])
    num_output     = int(argv[7])
    interleave_opt = int(argv[8])

    if (num_proc       <= 0) : raise ValueError("invalid value for num_proc")
    if (task_length    <  0) : raise ValueError("invalid value for task_length")
    if (read_buf       <= 0) : raise ValueError("invalid value for read_buf")
    if (write_buf      <= 0) : raise ValueError("invalid value for write_buf")
    if (num_input      <  0) : raise ValueError("invalid value for num_input")
    if (num_output     <  0) : raise ValueError("invalid value for num_output")
    if (interleave_opt <  0) : raise ValueError("invalid value for interleave_opt")

    if ( argc < (9 + num_input + (2*num_output)) ):
        raise ValueError("insufficient arguments")

  # print "task type     : %s" & type
  # print "num_processes : %d" & num_proc
  # print "task_length   : %d" & task_length
  # print "read_buf      : %d" & read_buf
  # print "write_buf     : %d" & write_buf
  # print "num_input     : %d" & num_input
  # print "num_output    : %d" & num_output
  # print "interleave_opt: %d" & interleave_opt

    input_names  = list()
    output_names = list()
    output_sizes = list()

    for i in range(num_input):

        input_names.append(argv[9+i])
        # print "%dth input file name: %s" % (i, input_names[i])

    for i in range(num_output):
        output_names.append(str(argv[9+num_input+i*2]))
        output_sizes.append(int(argv[10+num_input+i*2]))
      # print "%dth output file name: %s, size: %d" % \
      #       (i, output_names[i], # output_sizes[i])
    }


    if not interleave_opt:

        if master:
            print "process %d/%d reading input files" % (rank, size)

         # read input files*/
         readfiles(input_names, read_buf)

        if mpi:
            comm.Barrier()

        if master:
            print "process %d/%d is sleeping" % (rank, size)

        # compute
        compute(task_length)

        comm.Barrier()

        if master:
            print "process %d/%d writing output files" % (rank, scale)

        # write output files
        writefiles(output_names, output_sizes, write_buf)


    # interleave == 1 means we interleave input and compute, then write files at last
    elif interleave_opt == 1:
    
        if master:
            print "interleave input and compute, then write"

        # interleaved read and compute
        read_compute(input_names, read_buf, num_input, task_length)

        # write
        writefiles(output_names, output_sizes, write_buf)


    elif interleave_opt == 2:

        if master:
            print "read input, then interleave compute and write"

        readfiles(input_names, read_buf)
        compute_write(task_length, output_names, output_sizes, write_buf, num_output)


    elif interleave_opt == 3:

        print "interleave input, compute, and write"
        read_compute_write(input_names, read_buf, num_input, task_length, 
                           output_names, output_sizes, write_buf, num_output);
    }

    return 0


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    sys.exit main(sys.argv)


# ------------------------------------------------------------------------------

