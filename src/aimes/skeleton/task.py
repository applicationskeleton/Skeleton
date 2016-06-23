#!/usr/bin/env python

import os
import sys
import stat
import time
import pprint

import radical.utils   as ru
import radical.synapse as rs

# ------------------------------------------------------------------------------
#
mpi = False

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
def readfiles(input_files, bufsize):

    samples = list()
    n       = 0

    for input_file in input_files:

        n    += 1
        fsize = os.stat(input_file).st_size
        samples.append(['sto', n,  {'src'        : input_file, 
                                    'rsize'      : fsize, 
                                    'tgt'        : None, 
                                    'wsize'      : 0, 
                                    'buf'        : bufsize}])

    info, ret, out = rs.emulate(samples=samples)
  # pprint.pprint(info)


# ------------------------------------------------------------------------------
#
def writefiles(output_files, output_sizes, bufsize):

    samples = list()
    n       = 0

    for output_file, output_size in zip(output_files, output_sizes):

        n += 1
        samples.append(['sto', n,  {'src'        : None, 
                                    'rsize'      : 0, 
                                    'tgt'        : output_file, 
                                    'wsize'      : output_size, 
                                    'buf'        : bufsize}])

    info, ret, out = rs.emulate(samples=samples)
  # pprint.pprint(info)




# ------------------------------------------------------------------------------
#
def compute(task_length):

    samples = list()
    samples.append(['cpu', 0, {'time'       : task_length, 
                               'flops'      : 0, 
                               'efficiency' : 1}])

    info, ret, out = rs.emulate(samples=samples)
  # pprint.pprint(info)


# ------------------------------------------------------------------------------
#
def read_compute(input_files, bufsize, task_length):

    total_size = 0
    for input_file in input_files:
        total_size += os.stat(input_file).st_size

    nreads    = int(total_size / bufsize)
    cpu_chunk = int(task_length / nreads)

    samples = list()
    n       = 0

    for input_file in input_files:

        fsize     = os.stat(input_file).st_size / bufsize
        n_samples = fsize / bufsize

        for s in range(n_samples):

            # construct artificial time)
            n += 1
            samples.append(['cpu', t, {'time'       : cpu_chunk, 
                                       'flops'      : 0, 
                                       'efficiency' : 1}])

            samples.append(['sto', t,  {'src'        : input_file, 
                                        'rsize'      : bufsize, 
                                        'tgt'        : None, 
                                        'wsize'      : 0, 
                                        'buf'        : bufsize}])

    info, ret, out = rs.emulate(samples=samples)
  # pprint.pprint(info)


# ------------------------------------------------------------------------------
#
def compute_write(task_length, output_files, output_sizes, bufsize):

    total_size = 0
    for output_size in output_sizes:
        total_size += output_size

    nreads    = int(total_size / bufsize)
    cpu_chunk = int(task_length / nreads)

    samples = list()
    n       = 0

    for output_file, output_size in zip(output_files, output_sizes):

        fsize     = output_size
        n_samples = fsize / bufsize

        for s in range(n_samples):

            n += 1
            samples.append(['cpu', n, {'time'       : cpu_chunk, 
                                       'flops'      : 0, 
                                       'efficiency' : 1}])

            samples.append(['sto', n,  {'src'        : None, 
                                        'rsize'      : 0, 
                                        'tgt'        : output_size, 
                                        'wsize'      : bufsize, 
                                        'buf'        : bufsize}])

    info, ret, out = rs.emulate(samples=samples)
  # pprint.pprint(info)


# ------------------------------------------------------------------------------
#
def read_compute_write(input_files, r_bufsize, 
                       task_length, 
                       output_files, output_sizes, w_bufsize):

    # we chunk the compute according to output chunking
    total_size = 0
    for output_size in output_sizes:
        total_size += output_size

    nreads    = int(total_size / bufsize)
    cpu_chunk = int(task_length / nreads)

    samples = list()
    n       = 0

    for input_file, output_file, output_size in zip(input_files, output_files, output_sizes):

        r_size    = os.stat(input_file).st_size
        w_size    = output_size
        n_samples = fsize / bufsize

        for s in range(n_samples):

            n += 2
            samples.append(['sto', t,  {'src'          : input_file, 
                                        'rsize'        : int(r_size/n_samples), 
                                        'tgt'          : None, 
                                        'wsize'        : 0, 
                                        'buf'          : r_bufsize}])

            samples.append(['cpu', t+1, {'time'        : cpu_chunk, 
                                         'flops'       : 0, 
                                         'efficiency'  : 1}])

            samples.append(['sto', t+1,  {'src'        : None, 
                                          'rsize'      : 0, 
                                          'tgt'        : output_file, 
                                          'wsize'      : int(w_size/n_samples), 
                                          'buf'        : w_bufsize}])

    info, ret, out = rs.emulate(samples=samples)
  # pprint.pprint(info)


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
        rank   = 1
        size   = 1


    if argc < 9:
        raise ValueError("insufficient arguments")

    # input parameter processing
    type_s         = str(argv[1])
    num_proc       = int(argv[2])
    task_length    = float(argv[3])
    read_buf       = int(argv[4])
    write_buf      = int(argv[5])
    num_input      = int(argv[6])
    num_output     = int(argv[7])
    interleave_opt = int(argv[8])

    if num_proc       <= 0 : raise ValueError("invalid value for num_proc")
    if task_length    <  0 : raise ValueError("invalid value for task_length")
    if read_buf       <= 0 : raise ValueError("invalid value for read_buf")
    if write_buf      <= 0 : raise ValueError("invalid value for write_buf")
    if num_input      <  0 : raise ValueError("invalid value for num_input")
    if num_output     <  0 : raise ValueError("invalid value for num_output")
    if interleave_opt <  0 : raise ValueError("invalid value for interleave_opt")

    if argc < (9 + num_input + (2*num_output)):
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

        if mpi:
            comm.Barrier()

        if master:
            print "process %d/%d writing output files" % (rank, size)

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

    return 0


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    sys.exit(main(sys.argv))


# ------------------------------------------------------------------------------

