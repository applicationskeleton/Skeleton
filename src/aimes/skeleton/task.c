
/*
__author__    = "See AUTHORS file"
__copyright__ = "Copyright 2013-2015, The AIMES Project"
__license__   = "MIT"
*/


#include <math.h>
#include <time.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <libgen.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#ifdef MPI
# include <mpi.h>
#endif

#define MAXSTR    1024
#define MAXOUTPUT 1024


/* -------------------------------------------------------------------------- */
/* Declare write element */
struct we
{
    int fd;
    int size;
    char* fname;
};
typedef struct we WE;


/* -------------------------------------------------------------------------- */
void rand_str(char *dest, size_t length) {
    char charset[] = "0123456789"
                     "abcdefghijklmnopqrstuvwxyz"
                     "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    while (length-- > 0) {
        size_t index = (double) rand() / RAND_MAX * (sizeof charset - 1);
        *dest++ = charset[index];
    }
    *dest = '\0';
}


/* -------------------------------------------------------------------------- */
void bail (const char* fmt, ... )
{
    va_list va;
    va_start (va, fmt);

    fprintf  (stderr, "\nERROR: ");
    vfprintf (stderr, fmt, va);
    fprintf  (stderr, "\n       %s\n\n", strerror (errno));

    exit (-1);

}


/* -------------------------------------------------------------------------- */
void prep_output (char* fname)
{
    int   ret;
    char *dir;
    char *base;
    char *bname;
    char *dname;
	struct stat st;

    dir   = strdup   (fname);
    base  = strdup   (fname);

    dname = dirname  (dir);
    // bname = basename (base);

    ret = mkdir (dname, S_IRUSR|S_IWUSR|S_IXUSR);

    if ( ret < 0 )
	  if( errno != EEXIST )
		bail ("Cannot mkdir (%s)", dname);

	ret = chmod (dname, S_IRUSR|S_IWUSR|S_IXUSR);

    if ( ret < 0 )
	  bail ("Cannot chmod 700 (%s)", dname);

    return;
}


/* -------------------------------------------------------------------------- */
void readfiles(char **input_files, int bufsize, int num_input)
{
    int   i;
    int   ret;
    char* buffer = malloc(bufsize*sizeof(char));

    if ( NULL == buffer )
        bail ("cannot allocate buffer (%d bytes)", bufsize);

    for(i=0; i<num_input; i++)
    {
        int fd;
        int size;
        int count = 0;
        long int total_size = 0;
        struct stat st;

        fd = open(input_files[i], O_RDONLY);

        if ( fd < 0)
            bail ("cannot open %s", input_files[i]);

        ret = stat(input_files[i], &st);

        if ( ret < 0 )
            bail ("cannot stat %s", input_files[i]);

        printf("reading input file: %s size: %ld num_reads: %d\n", 
               input_files[i], (long)st.st_size, 
               (int)ceil((long)st.st_size/bufsize));

        while (total_size < (long)st.st_size)
        {
            memset(buffer, '\0', bufsize);

            if (st.st_size - total_size > bufsize)
                size = read(fd, buffer, bufsize);
            else
                size = read(fd, buffer, st.st_size-total_size);

            if ( size < 0 )
                bail ("cannot read from %s", input_files[i]);

            //printf("read: %s\n", buffer);
            total_size = total_size + size;
            count++;

            if (count % 1000 == 0)
                printf("reading operations: %d total_size: %ld st_size: %ld\n", 
                       count, total_size, (long)st.st_size);
        }

        printf("read : %ld bytes to %s with bufsize: %d\n", total_size, input_files[i], bufsize);

        close(fd);
    }

    return;
}


/* -------------------------------------------------------------------------- */
void writefiles(char **output_files, int *output_sizes, int bufsize, int num_output)
{
    int   i;
    char* buffer = malloc(bufsize*sizeof(char));

    if ( NULL == buffer )
        bail ("cannot allocate buffer (%d bytes)", bufsize);

    for (i=0; i<num_output; i++)
    {
        int fd;
        int size;
        int count = 0;
        long int total_size = 0;

        prep_output (output_files[i]);

        mode_t mode = S_IRUSR|S_IWUSR;
        fd = open(output_files[i], O_CREAT|O_TRUNC|O_RDWR, mode);

        if ( fd < 0)
            bail ("cannot open %s", output_files[i]);

        while (total_size < output_sizes[i])
        {
            rand_str(buffer, bufsize);
            if (output_sizes[i] - total_size > bufsize)
                size = write(fd, buffer, bufsize);
            else
                size = write(fd, buffer, output_sizes[i]-total_size);

            if ( size < 0 )
                bail ("cannot write to %s", output_files[i]);

            sync();
            total_size = total_size + size;
            count++;

            if (count % 1000 == 0)
                printf("write operations: %d total_size: %ld output_size: %d\n", 
                       count, total_size, output_sizes[i]);
        }

        printf("write: %ld bytes to %s with bufsize: %d\n", total_size, output_files[i], bufsize);

        close(fd);
    }

    return;
}


/* -------------------------------------------------------------------------- */
void compute(double task_length)
{
    int ret;
    struct timespec tim;
    double sleep_interval = (double)task_length;
    printf("sleep interval: %f\n", sleep_interval);

    tim.tv_sec  = floor(sleep_interval);
    tim.tv_nsec = (sleep_interval - tim.tv_sec)*1000000000;

    ret = nanosleep(&tim, NULL);

    if ( ret < 0 )
        bail ("cannot sleep");

    return;
}

/* --------------------------------------------------------------------------
 * Run floating point calculations as a token of work
 * compute_flop with run task_length number of floating point
 * operations
 */
void compute_flop(double task_length)
{
    int ret;
    int i;
    double a = 0.0;
    double b = 1.0;
    double c;
    struct timespec tim;

    if ( (int)task_length < 0 ) bail ("task_length must be > 0");

    printf("floating point operations : %f\n", task_length);

    // Simple fibonacci generation
    for ( i = 0; i < int(task_length) ; i++ ){
        c = a+b;
        a = b;
        b = c;
    }

    return;
}


/* -------------------------------------------------------------------------- */
void read_compute(char **input_files, int bufsize, int num_input, double task_length)
{
    int i;
    int ret;
    char* buffer = malloc(bufsize*sizeof(char));
    float num_reads = 0.0;

    if ( NULL == buffer )
        bail ("cannot allocate buffer (%d bytes)", bufsize);

    for(i=0; i<num_input; i++)
    {
        int fd;
        int size;
        struct stat st;

        fd = open(input_files[i], O_RDONLY);

        if ( fd < 0)
            bail ("cannot open %s", input_files[i]);

        ret = stat(input_files[i], &st);

        if ( ret < 0 )
            bail ("cannot stat %s", input_files[i]);

        if (st.st_size < bufsize)
        {
            num_reads++;
        }
        else
        {
            num_reads = num_reads + ceil((double)(st.st_size/(double)bufsize));
        }

        close(fd);
    }
    printf("total read operations: %d\n", (int)num_reads);

    struct timespec tim;
    double sleep_interval = (double)(task_length/(double)num_reads);
    printf("sleep interval: %f\n", sleep_interval);

    tim.tv_sec = floor(sleep_interval);
    tim.tv_nsec = (sleep_interval - tim.tv_sec)*1000000000;

    for (i=0; i<num_input; i++)
    {
        int fd;
        int size;
        long int total_size = 0;
        struct stat st;

        fd = open(input_files[i], O_RDONLY);

        if ( fd < 0)
            bail ("cannot open %s", input_files[i]);

        stat(input_files[i], &st);

        if ( ret < 0 )
            bail ("cannot stat %s", input_files[i]);

        printf("reading input file: %s size: %ld\n", input_files[i], (long)st.st_size);

        while (total_size < st.st_size)
        {
            memset(buffer, '\0', bufsize);
            if(st.st_size - total_size > bufsize)
                size = read(fd, buffer, bufsize);
            else
                size = read(fd, buffer, st.st_size-total_size);

            if ( size < 0 )
                bail ("cannot read from %s", input_files[i]);

            total_size = total_size + size;
            ret        = nanosleep(&tim, NULL);

            if ( ret < 0 )
                bail ("cannot sleep");
        }
    }

    return;
}

/* -------------------------------------------------------------------------- */
void compute_write (double task_length, char **output_files, int *output_sizes, 
                    int bufsize, int num_output)
{
    /*Calculate how many writes we need in total*/
    int i;
    int ret;
    int num_writes = 0;

    for (i=0; i<num_output; i++)
    {
        num_writes = num_writes + ceil(output_sizes[i]/(double)bufsize);
    }

    /*calculate sleep interval*/
    struct timespec tim;
    double sleep_interval = (double)(task_length/(double)num_writes);

    printf("sleep interval: %f\n", sleep_interval);

    tim.tv_sec = floor(sleep_interval);
    tim.tv_nsec = (sleep_interval - tim.tv_sec)*1000000000;

    /*Interleaved compute and write*/
    char* buffer = malloc(bufsize*sizeof(char));

    if ( NULL == buffer )
        bail ("cannot allocate buffer (%d bytes)", bufsize);

    memset(buffer, 'a', bufsize);

    for (i=0; i<num_output; i++)
    {
        int fd;
        int size;
        int count = 0;
        long int total_size = 0;

        prep_output (output_files[i]);

        mode_t mode = S_IRUSR|S_IWUSR;
        fd = open(output_files[i], O_CREAT|O_TRUNC|O_RDWR, mode);

        if ( fd < 0)
            bail ("cannot open %s", output_files[i]);

        while (total_size < output_sizes[i])
        {
            ret = nanosleep(&tim, NULL);

            if ( ret < 0 )
                bail ("cannot sleep");

            if(output_sizes[i] - total_size > bufsize)
                size = write(fd, buffer, bufsize);
            else
                size = write(fd, buffer, output_sizes[i]-total_size);

            if ( size < 0 )
                bail ("cannot write to %s", output_files[i]);

            // FIXME: why not sync here? the plain write op syncs...
            // sync();
            total_size = total_size + size;
            count++;

            if (count % 1000 == 0)
                printf("write operations: %d total_size: %ld output_size: %d\n", 
                       count, total_size, output_sizes[i]);
        }

        printf("write: %ld bytes to %s with bufsize: %d\n", total_size, output_files[i], bufsize);

        close(fd);
    }

    return;
}


/* -------------------------------------------------------------------------- */
void read_compute_write(char **input_files, int r_bufsize, int num_input, 
                        double task_length, char **output_files, int *output_sizes, 
                        int w_bufsize, int num_output)
{
    int i;
    int ret;
    int num_reads = 0;
    char* buffer = malloc(r_bufsize*sizeof(char));

    if ( NULL == buffer )
        bail ("cannot allocate buffer (%d bytes)", r_bufsize);

    for (i=0; i<num_input; i++)
    {
        int fd;
        int size;
        struct stat st;

        fd = open(input_files[i], O_RDONLY);

        if ( fd < 0)
            bail ("cannot open %s", input_files[i]);

        stat(input_files[i], &st);

        if ( ret < 0 )
            bail ("cannot stat %s", input_files[i]);

        if (st.st_size < r_bufsize)
        {
            num_reads++;
        }
        else
        {
            num_reads = num_reads + ceil(st.st_size/(double)r_bufsize);
        }

        close(fd);
    }

    printf("total read operations: %d\n", num_reads);

    struct timespec tim;
    double sleep_interval = (double)(task_length/(double)num_reads);

    printf("sleep interval: %f\n", sleep_interval);

    tim.tv_sec = floor(sleep_interval);
    tim.tv_nsec = (sleep_interval - tim.tv_sec)*1000000000;

    /*Calculate how many writes we need in total*/
    int num_writes = 0;
    for (i=0; i<num_output; i++)
    {
        num_writes = num_writes + ceil(output_sizes[i]/(double)w_bufsize);
    }

    WE* wel = malloc(num_writes*sizeof(WE));

    if ( NULL == wel )
        bail ("cannot allocate buffer (%d bytes)", num_writes*sizeof(WE));

    int write_id = 0;
    for(i=0; i<num_output; i++)
    {
        int fd;
        int size;
        long int total_size = 0;

        prep_output (output_files[i]);

        mode_t mode = S_IRUSR|S_IWUSR;
        fd = open(output_files[i], O_CREAT|O_TRUNC|O_RDWR, mode);

        if ( fd < 0)
            bail ("cannot open %s", output_files[i]);

        printf("output filename: %s, output file size: %d\n", output_files[i], output_sizes[i]);

        while(total_size < output_sizes[i])
        {
            WE element;
            element.fd = fd;
            element.fname = output_files[i];

            if (output_sizes[i] - total_size > w_bufsize)
            {
                element.size = w_bufsize;
                total_size   = total_size + w_bufsize;
            }
            else
            {
                element.size = output_sizes[i]-total_size;
                total_size   = output_sizes[i];
            }

            wel[write_id++] = element;
        }
    }  

    for (i=0; i<num_writes; i++){
        printf("fd: %d, size: %d\n", wel[i].fd, wel[i].size);
    }

    /*read-compute-write*/
    char* wbuffer = malloc(w_bufsize*sizeof(char));

    if ( NULL == wbuffer )
        bail ("cannot allocate buffer (%d bytes)", w_bufsize);

    memset(wbuffer, 'a', w_bufsize);

    int write_seq = 0;
    for(i=0; i<num_input; i++)
    {
        int fd;
        int size;
        long int total_size = 0;
        struct stat st;

        fd = open(input_files[i], O_RDONLY);

        if ( fd < 0)
            bail ("cannot open %s", input_files[i]);

        stat(input_files[i], &st);

        if ( ret < 0 )
            bail ("cannot stat %s", input_files[i]);

        printf("reading input file: %s size: %ld\n", input_files[i], (long)st.st_size);

        while (total_size < st.st_size)
        {
            memset(buffer, '\0', r_bufsize);

            if(st.st_size - total_size > r_bufsize)
                size = read(fd, buffer, r_bufsize);
            else
                size = read(fd, buffer, st.st_size-total_size);

            if ( size < 0 )
                bail ("cannot read from %s", input_files[i]);

            total_size = total_size + size;
            ret = nanosleep(&tim, NULL);

            if ( ret < 0 )
                bail ("cannot sleep");

            printf("num_writes: %d, num_reads: %d, iteration: %d\n", num_writes, 
                   num_reads, (int)ceil(num_writes/(double)num_reads));

            int j;
            for (j=0; j<(int)ceil(num_writes/(double)num_reads); j++)
            {
                printf("write fd %d, size: %d\n", wel[write_seq].fd, wel[write_seq].size);
                size = write(wel[write_seq].fd, wbuffer, wel[write_seq].size);

                if ( size < 0 )
                    bail ("cannot write to %s", wel[write_seq].fname);

                // FIXME: why not sync here as in the plain write version?
                //sync();
                write_seq++;
            }

            num_writes = num_writes - j;
            num_reads  = num_reads  - 1;
        }
    }

    for (i=0; i<num_writes; i++){
        close (wel[i].fd);
    }

    return;
}


/* -------------------------------------------------------------------------- */
int main(int argc, char **argv)
{
    int i;
#ifdef MPI
    printf("parallel: %d\n", argc);
    int rank, scale;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &scale);
#else
    printf("serial: %d\n", argc);
#endif

    if ( argc < 9 )
        bail ("insufficient arguments");

    /*Input parameter processing*/
    char* type         =      argv[1];
    int num_proc       = atoi(argv[2]);
    double task_length = atof(argv[3]);
    int read_buf       = atoi(argv[4]);
    int write_buf      = atoi(argv[5]);
    int num_input      = atoi(argv[6]);
    int num_output     = atoi(argv[7]);
    int interleave_opt = atoi(argv[8]);

    if (num_proc       <= 0) bail ("invalid value for num_proc");
    if (task_length    <  0) bail ("invalid value for task_length");
    if (read_buf       <= 0) bail ("invalid value for read_buf");
    if (write_buf      <= 0) bail ("invalid value for write_buf");
    if (num_input      <  0) bail ("invalid value for num_input");
    if (num_output     <  0) bail ("invalid value for num_output");
    if (interleave_opt <  0) bail ("invalid value for interleave_opt");

    if ( argc < (9 + num_input + (2*num_output)) )
        bail ("insufficient arguments");

    // printf("task type     : %s\n", type);  
    // printf("num_processes : %d\n", num_proc);
    // printf("task_length   : %d\n", task_length);
    // printf("read_buf      : %d\n", read_buf);
    // printf("write_buf     : %d\n", write_buf);
    // printf("num_input     : %d\n", num_input);
    // printf("num_output    : %d\n", num_output);
    // printf("interleave_opt: %d\n", interleave_opt);

    char** input_names;
    input_names = malloc(num_input*sizeof(char)*MAXSTR);

    if ( NULL == input_names )
        bail ("cannot alloc input_names");

    for (i=0; i<num_input; i++)
    {
        input_names[i] = argv[9+i];
        //printf("%dth input file name: %s\n", i, input_names[i]);
    }

    char** output_names;
    output_names = malloc(num_output*sizeof(char)*MAXSTR);

    if ( NULL == output_names )
        bail ("cannot alloc output_names");

    int* output_sizes;
    output_sizes = malloc(num_output*sizeof(int));

    if ( NULL == output_sizes )
        bail ("cannot alloc output_sizes");

    for (i=0; i<num_output; i++)
    { 
        output_names[i] = argv[9+num_input+i*2];
        output_sizes[i] = atoi(argv[10+num_input+i*2]);
        //printf("%dth output file name: %s, size: %d\n", i, output_names[i], output_sizes[i]);
    }


    if (interleave_opt == 0)
    {
#ifdef MPI
        // FIXME: why is only rank 0 ever doing anything?  That will miss I/O
        // contention, for example...
        if(rank==0)
        {
            printf("process %d/%d reading input files\n", rank, scale);
#endif

            /*read input files*/
            readfiles(input_names, read_buf, num_input);

#ifdef MPI
        }
        MPI_Barrier(MPI_COMM_WORLD);
#endif

#ifdef MPI
        printf("process %d/%d is sleeping\n", rank, scale);
#endif

        /*compute*/
        compute(task_length);

#ifdef MPI
        MPI_Barrier(MPI_COMM_WORLD);

        if(rank==0)
        {
            printf("process %d/%d writing output files\n", rank, scale);
#endif

            /*write output files*/
            writefiles(output_names, output_sizes, write_buf, num_output);

#ifdef MPI
        }
        MPI_Finalize();
#endif
    }

    /*interleave==1 means we interleave input and compute, then write files at last*/
    else if(interleave_opt == 1)
    {
        printf("interleave input and compute, then write\n");

        /*Interleaved read and compute*/
        read_compute(input_names, read_buf, num_input, task_length);

        /*Write*/
        writefiles(output_names, output_sizes, write_buf, num_output);
    }
    else if(interleave_opt == 2)
    {
        printf("read input, then interleave compute and write\n");
        readfiles(input_names, read_buf, num_input);
        compute_write(task_length, output_names, output_sizes, write_buf, num_output);
    }
    else if(interleave_opt == 3)
    {
        printf("interleave input, compute, and write\n");
        read_compute_write(input_names, read_buf, num_input, task_length, 
                           output_names, output_sizes, write_buf, num_output);
    }

    /*before exiting, free all the memory we have allocated*/
    free(input_names);
    free(output_names);
    free(output_sizes);
}

