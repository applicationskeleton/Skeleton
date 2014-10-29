#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <math.h>
#include <time.h>
#ifdef MPI
#include <mpi.h>
#endif

#define MAXSTR 1024
#define MAXOUTPUT 1024

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

/*Declare write element*/
struct we
{
  int fd;
  int size;
};
typedef struct we WE;

int readfiles(char **input_files, int buf, int num_input)
{
  char* buffer = malloc(buf*sizeof(char));
  int i;
  for(i=0; i<num_input; i++)
  {
    int fd;
    int size;
    long int total_size = 0;
    int count=0;
    struct stat st;

    fd = open(input_files[i], O_RDONLY);
    stat(input_files[i], &st);
    printf("reading input file: %s size: %ld num_reads: %d\n", input_files[i], (long)st.st_size, (int)ceil((long)st.st_size/buf));
    while (total_size < (long)st.st_size)
    {
      memset(buffer, '\0', buf);
      if(st.st_size - total_size > buf)
	size = read(fd, buffer, buf);
      else
	size = read(fd, buffer, st.st_size-total_size);
      //printf("read: %s\n", buffer);
      total_size = total_size + size;
      count=count+1;
      if(count%1000 == 0)
	printf("reading operations: %d total_size: %ld st_size: %ld\n", count, total_size, (long)st.st_size);
    }
    close(fd);
  }
  return 0;
}

int writefiles(char **output_files, int *output_sizes, int buf, int num_output)
{
  char* buffer = malloc(buf*sizeof(char));

  //memset(buffer, 'a', buf);
  int i;
  for(i=0; i<num_output; i++)
  {
    int fd;
    int size;
    long int total_size = 0;
  
    mode_t mode = S_IRUSR|S_IWUSR;
    fd = open(output_files[i], O_CREAT|O_TRUNC|O_RDWR, mode);
   
    while(total_size < output_sizes[i])
    {
      rand_str(buffer, buf);
      if(output_sizes[i] - total_size > buf)
	size = write(fd, buffer, buf);
      else
	size = write(fd, buffer, output_sizes[i]-total_size);
      sync();
      total_size = total_size + size;
    }
    printf("write: %ld bytes to %s with buf size: %d\n", total_size, output_files[i], buf);
    close(fd);
  }
  return 0;
}

int compute(double task_length)
{
  struct timespec tim;
  double sleep_interval = (double)task_length;
  printf("sleep interval: %f\n", sleep_interval);

  tim.tv_sec = floor(sleep_interval);
  tim.tv_nsec = (sleep_interval - tim.tv_sec)*1000000000;

  nanosleep(&tim, NULL);
  return 0;
}

int read_compute(char **input_files, int buf, int num_input, double task_length)
{
  char* buffer = malloc(buf*sizeof(char));
  int i;
  float num_reads = 0.0;
  for(i=0; i<num_input; i++)
  {
    int fd;
    int size;
    struct stat st;

    fd = open(input_files[i], O_RDONLY);
    stat(input_files[i], &st);

    if(st.st_size < buf)
    {
      num_reads = num_reads + 1;
    }
    else
    {
      num_reads = num_reads + ceil((double)(st.st_size/(double)buf));
    }
    close(fd);
  }
  printf("total read operations: %d\n", (int)num_reads);

  struct timespec tim;
  double sleep_interval = (double)(task_length/(double)num_reads);
  printf("sleep interval: %f\n", sleep_interval);

  tim.tv_sec = floor(sleep_interval);
  tim.tv_nsec = (sleep_interval - tim.tv_sec)*1000000000;
  

  for(i=0; i<num_input; i++)
  {
    int fd;
    int size;
    long int total_size = 0;
    struct stat st;

    fd = open(input_files[i], O_RDONLY);
    stat(input_files[i], &st);
    printf("reading input file: %s size: %ld\n", input_files[i], (long)st.st_size);
    while (total_size < st.st_size)
    {
      memset(buffer, '\0', buf);
      if(st.st_size - total_size > buf)
	size = read(fd, buffer, buf);
      else
	size = read(fd, buffer, st.st_size-total_size);
      total_size = total_size + size;
      nanosleep(&tim, NULL);
    }
  }
  return 0;
}

int compute_write(double task_length, char **output_files, int *output_sizes, int wbuf, int num_output)
{
  /*Calculate how many writes we need in total*/
  int i;
  int num_writes = 0;
  for(i=0; i<num_output; i++)
  {
    num_writes = num_writes + ceil(output_sizes[i]/(double)wbuf);
  }

  /*calculate sleep interval*/
  struct timespec tim;
  double sleep_interval = (double)(task_length/(double)num_writes);
  printf("sleep interval: %f\n", sleep_interval);

  tim.tv_sec = floor(sleep_interval);
  tim.tv_nsec = (sleep_interval - tim.tv_sec)*1000000000;

  /*Interleaved compute and write*/
  char* buffer = malloc(wbuf*sizeof(char));
  memset(buffer, 'a', wbuf);
  for(i=0; i<num_output; i++)
  {
    int fd;
    int size;
    long int total_size = 0;
  
    mode_t mode = S_IRUSR|S_IWUSR;
    fd = open(output_files[i], O_CREAT|O_TRUNC|O_RDWR, mode);
   
    while(total_size < output_sizes[i])
    {
      nanosleep(&tim, NULL);
      if(output_sizes[i] - total_size > wbuf)
	size = write(fd, buffer, wbuf);
      else
	size = write(fd, buffer, output_sizes[i]-total_size);
      //sync();
      total_size = total_size + size;
      printf("write: %d bytes to %s\n", size, output_files[i]);
    }
    close(fd);
    printf("totally write: %ld bytes to %s\n", total_size, output_files[i]);
  }
  return 0;
}

int read_compute_write(char **input_files, int buf, int num_input, double task_length, char **output_files, int *output_sizes, int wbuf, int num_output)
{
  char* buffer = malloc(buf*sizeof(char));
  int i;
  int num_reads = 0;
  for(i=0; i<num_input; i++)
  {
    int fd;
    int size;
    struct stat st;

    fd = open(input_files[i], O_RDONLY);
    stat(input_files[i], &st);

    if(st.st_size < buf)
    {
      num_reads = num_reads + 1;
    }
    else
    {
      num_reads = num_reads + ceil(st.st_size/(double)buf);
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
  for(i=0; i<num_output; i++)
  {
    num_writes = num_writes + ceil(output_sizes[i]/(double)wbuf);
  }
  WE* wel = malloc(num_writes*sizeof(WE));
  int write_id = 0;
  for(i=0; i<num_output; i++)
  {
    int fd;
    int size;
    long int total_size = 0;

    mode_t mode = S_IRUSR|S_IWUSR;
    fd = open(output_files[i], O_CREAT|O_TRUNC|O_RDWR, mode);
    printf("output filename: %s, output file size: %d\n", output_files[i], output_sizes[i]);
   
    while(total_size < output_sizes[i])
    {
      if(output_sizes[i] - total_size > wbuf)
      {
	WE element;
	element.fd = fd;
	element.size = wbuf;
	wel[write_id++] = element;
	total_size = total_size + wbuf;
      }
      else
      {
	WE element;
	element.fd = fd;
	element.size = output_sizes[i]-total_size;
	wel[write_id++] = element;
	total_size = output_sizes[i];
      }
    }
  }  
  
  for(i=0; i<num_writes; i++){
    printf("fd: %d, size: %d\n", wel[i].fd, wel[i].size);
  }
  
  /*read-compute-write*/
  char* wbuffer = malloc(wbuf*sizeof(char));
  memset(wbuffer, 'a', wbuf);

  int write_seq = 0;
  for(i=0; i<num_input; i++)
  {
    int fd;
    int size;
    long int total_size = 0;
    struct stat st;

    fd = open(input_files[i], O_RDONLY);
    stat(input_files[i], &st);
    printf("reading input file: %s size: %ld\n", input_files[i], (long)st.st_size);
    while (total_size < st.st_size)
    {
      memset(buffer, '\0', buf);
      if(st.st_size - total_size > buf)
	size = read(fd, buffer, buf);
      else
	size = read(fd, buffer, st.st_size-total_size);
      total_size = total_size + size;
      nanosleep(&tim, NULL);
      
      printf("num_writes: %d, num_reads: %d, iteration: %d\n", num_writes, num_reads, (int)ceil(num_writes/(double)num_reads));
      int j;
      for(j=0; j<(int)ceil(num_writes/(double)num_reads); j++)
      {
	printf("write fd %d, size: %d\n", wel[write_seq].fd, wel[write_seq].size);
	write(wel[write_seq].fd, wbuffer, wel[write_seq].size);
	//sync();
	write_seq = write_seq + 1;
      }
      num_writes = num_writes - j;
      num_reads = num_reads - 1;
    }
  }

  for(i=0; i<num_writes; i++){
    close(wel[i].fd);
  }
  return 0;
}

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

  /*Input parameter processing*/
  char* type = argv[1];
  //printf("task type: %s\n", type);  

  int num_proc = atoi(argv[2]);
  //printf("num_processes: %d\n", num_proc);

  double task_length = atof(argv[3]);
  //printf("task_length: %d\n", task_length);

  int read_buf = atoi(argv[4]);
  //printf("read_buf: %d\n", read_buf);

  int write_buf = atoi(argv[5]);
  //printf("write_buf: %d\n", write_buf);

  int num_input = atoi(argv[6]);
  //printf("num_input: %d\n", num_input);

  int num_output = atoi(argv[7]);
  //printf("num_output: %d\n", num_output);

  int interleave_opt = atoi(argv[8]);
  printf("interleave_opt: %d\n", interleave_opt);

  char** input_names;
  input_names = malloc(num_input*sizeof(char)*MAXSTR);
  for(i=0; i<num_input; i++)
  {
    input_names[i] = argv[9+i];
    //printf("%dth input file name: %s\n", i, input_names[i]);
  }

  char** output_names;
  output_names = malloc(num_output*sizeof(char)*MAXSTR);

  int* output_sizes;
  output_sizes = malloc(num_output*sizeof(int));
  for(i=0; i<num_output; i++)
  { 
    output_names[i] = argv[9+num_input+i*2];
    output_sizes[i] = atoi(argv[10+num_input+i*2]);
    //printf("%dth output file name: %s, size: %d\n", i, output_names[i], output_sizes[i]);
  }

  if(interleave_opt == 0)
  {
#ifdef MPI
    if(rank==0)
    {
      printf("process %d/%d reading input files\n", rank, scale);
#endif

      /*read input files*/
      int readbytes;
      readbytes = readfiles(input_names, read_buf, num_input);

#ifdef MPI
    }
    MPI_Barrier(MPI_COMM_WORLD);
#endif

#ifdef MPI
    printf("process %d/%d is sleeping\n", rank, scale);
#endif

    /*compute*/
    int ret;
    ret = compute(task_length);

#ifdef MPI
    MPI_Barrier(MPI_COMM_WORLD);

    if(rank==0)
    {
      printf("process %d/%d writing output files\n", rank, scale);
#endif

      /*write output files*/
      int writebytes;
      writebytes = writefiles(output_names, output_sizes, write_buf, num_output);

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
    int ret;
    ret = read_compute(input_names, read_buf, num_input, task_length);

    /*Write*/
    int writebytes;
    writebytes = writefiles(output_names, output_sizes, write_buf, num_output);
  }
  else if(interleave_opt == 2)
  {
    printf("read input, then interleave compute and write\n");
    int readbytes;
    readbytes = readfiles(input_names, read_buf, num_input);

    int ret;
    ret = compute_write(task_length, output_names, output_sizes, write_buf, num_output);
  }
  else if(interleave_opt == 3)
  {
    printf("interleave input, compute, and write\n");
    int ret;
    ret = read_compute_write(input_names, read_buf, num_input, task_length, output_names, output_sizes, write_buf, num_output);
  }
  /*before exiting, free all the memory we have allocated*/
  free(input_names);
  free(output_names);
  free(output_sizes);
}
