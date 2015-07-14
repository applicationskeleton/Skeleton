
__author__    = "See AUTHORS file"
__copyright__ = "Copyright 2013-2015, The AIMES Project"
__license__   = "MIT"

import os
import re
import sys
import copy
import random
import subprocess

# ------------------------------------------------------------------------------
#
class File(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, size):

        self.name = name
        self.size = size


# ------------------------------------------------------------------------------
#
class Task(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, taskid, task_type, processes, length, read_buf, write_buf, 
                 inputlist, outputlist, interleave_option, mode):

        self.taskid            = taskid
        self.task_type         = task_type
        self.processes         = processes
        self.length            = length
        self.read_buf          = read_buf
        self.write_buf         = write_buf
        self.inputlist         = inputlist
        self.outputlist        = outputlist
        self.interleave_option = interleave_option
        self.mode              = mode

        self.command           = None
      # self.path_to_binary    = ""
        self.args              = ""
        

    # --------------------------------------------------------------------------
    #
    def get_args(self):

        s = " ".join ([self.task_type, self.processes, self.length,
                       self.read_buf, str(self.write_buf), 
                       str(len(self.inputlist)), str(len(self.outputlist)), 
                               self.interleave_option])

        for i in self.inputlist:
            s += " %s" % i.name

        for o in self.outputlist:
            s += " %s %s" % (o.name, o.size)

        self.args = s
        return s
    

    # --------------------------------------------------------------------------
    #
    def get_command(self, stage) :

        s  = "task "
        s += " ".join ([self.task_type, self.processes, self.length,
                        self.read_buf,  str(self.write_buf), 
                        str(len(self.inputlist)), str(len(self.outputlist)), 
                        self.interleave_option])

        for i in range(len(self.inputlist)):
            s += " %s/%s" % (stage.inputdir[i], self.inputlist[i].name)

        for o in range(len(self.outputlist)):
            s += " %s/%s %s" % (stage.outputdir[o], 
                                self.outputlist[o].name, 
                                self.outputlist[o].size)

        self.command = s
        return s
        

    # --------------------------------------------------------------------------
    #
    def __str__ (self):    

        return self.get_command ()


    # --------------------------------------------------------------------------
    #
    def dump (self) :
        print str(self)


# ------------------------------------------------------------------------------
#
class Stage():

    # --------------------------------------------------------------------------
    #
    def __init__ (self, name, task_type, num_tasks, length_para, 
                  processes, read_buf, write_buf, 
                  input_per_task,  input_para,  input_task_mapping, 
                  output_per_task, output_para, interleave_option, 
                  iter_num, iter_stages, iter_sub, mode):
        '''
        length_para can be "function parameters";
        input_para contains [source, size],
        source can be "filesystem or Stage_X.output",
        size can be "function parameters".
        '''
        self.mode               = mode
        self.name               = name
        self.task_type          = task_type
        self.num_tasks          = int(num_tasks)
        self.length_para        = length_para
        self.processes          = processes
        self.read_buf           = read_buf
        self.write_buf          = write_buf
        self.input_per_task     = input_per_task
        self.input_para         = input_para
        self.input_task_mapping = input_task_mapping
        self.output_per_task    = output_per_task
        self.output_para        = output_para
        self.interleave_option  = interleave_option
        self.iter_num           = iter_num
        self.iter_stages        = iter_stages
        self.iter_sub           = iter_sub

        self.task_list = []
        self.inputdir  = []
        self.outputdir = []


    # --------------------------------------------------------------------------
    #
    def __str__ (self):
        return self.name


    # --------------------------------------------------------------------------
    #
    def dump (self) :
        print str(self)



# ------------------------------------------------------------------------------
#
class Application(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, input_file):

        self.name       = name
        self.input_file = input_file
        self.num_stage  = 0
        self.stagelist  = []


    # --------------------------------------------------------------------------
    #
    def setdir(self):

        for s in self.stagelist:

            for i in s.task_list[0].inputlist:
                fname = i.name
                words = fname.split('_')
                if len(words) > 5:
                    dir_name = words[0]+'_'+words[1]+'_'+words[2]+'_'+words[3]+'_'+words[4]
                else:
                    dir_name = words[0]+'_'+words[1]+'_'+words[2]
                s.inputdir.append(dir_name)

            for o in s.task_list[0].outputlist:
                fname = o.name
                words = fname.split('_')
                if len(words) > 5:
                    dir_name = words[0]+'_'+words[1]+'_'+words[2]+'_'+words[3]+'_'+words[4]
                else:
                    dir_name = words[0]+'_'+words[1]+'_'+words[2]
                s.outputdir.append(dir_name)


    # --------------------------------------------------------------------------
    #
    def getSetup(self):
        """
        returns list of command lines which, when run, setup all files and dirs
        for the first stage
        """

        ret    = []
        dirmap = {}

        for stage in self.stagelist:
            for i in range(len(stage.input_para)):
                if stage.input_para[i][0] == "filesystem" and stage.inputdir[i] not in dirmap:
                    ret.append ("mkdir -p %s" % stage.inputdir[i])
                    dirmap[stage.inputdir[i]] = 1
                for d in stage.outputdir: 
                    if d not in dirmap:
                        ret.append ("mkdir -p %s" % d)

                        dirmap[d] = 1

        filemap = dict()
        for stage in self.stagelist:
            for i in range(len(stage.input_para)):
                if stage.input_para[i][0] == "filesystem":
                    for t in stage.task_list:
                        if t.inputlist[i].name not in filemap:
                            ret.append ("dd if=/dev/zero of='%s/%s'  bs='%s' count=1" \
                                     % (stage.inputdir[i], t.inputlist[i].name, t.inputlist[i].size))
                            filemap[t.inputlist[i].name] = 1

        return ret


    # --------------------------------------------------------------------------
    #
    def printSetup(self):

        prefix   = os.path.basename(self.input_file).split('.')[0]
        filename = prefix+"_Setup.sh"

        with open(filename, 'w+') as fd :
            setup = self.getSetup ()
            for cmd in setup :
                fd.write ("%s\n" % cmd)


    # --------------------------------------------------------------------------
    #
    def resolve_iteration(self):

        rlist = []
        self.stagelist.reverse()

        while(len(self.stagelist)>0):
            s = self.stagelist.pop()
            if s.iter_num == 1:
                rlist.append(s)
            else:
                iter_stages=[]
                stage_index = []
                stage_names = s.iter_stages.split(', ')
                stage_names.remove(s.name)
                iter_stages.append(s)
                stage_index.append(s.name)
                for ss in stage_names:
                    popstage = self.stagelist.pop()
                    iter_stages.append(popstage)
                    stage_index.append(popstage.name)
                for i in range(int(s.iter_num)):
                    for stage in iter_stages:
                        r_stage = copy.deepcopy(stage)
                        if i < int(s.iter_num)-1:
                            r_stage.name = stage.name+"_Iter_"+str(i+1)

                        #change the input file sources accordingly
                        #i == 0 and iter_stage.index(stage)==0 means this is the first stage of iteration
                        #and we do not have to change the input sources
                        if i == 0 and iter_stages.index(stage)==0: 
                            pass
                        #i == int(s.iter_num)-1 and iter_stages.index(stage) == len(iter_stages)-1
                        #means this stage is the very last stage of the iteration
                        elif i == int(s.iter_num)-1 and iter_stages.index(stage) == len(iter_stages)-1:
                            if len(iter_stages) > 1:
                                pass
                            else:
                                src = stage.iter_sub[0]
                                tar = stage.iter_sub[1]
                                inputid = int(src.split('_')[2])
                                stage, usage = tar.split('.')
                                inputsrc = stage+"_Iter_"+str(i)+"."+usage
                                r_stage.input_para[inputid-1][0] = inputsrc

                        #i > 0 menas this is a latter stage
                        #and we need to change their input file sources
                        else:
                            #if this is the beginning stage of the iteration
                            if len(stage.iter_sub) > 0:
                                src = stage.iter_sub[0]
                                tar = stage.iter_sub[1]
                                inputid = int(src.split('_')[2])
                                stage, usage = tar.split('.')
                                inputsrc = stage+"_Iter_"+str(i)+"."+usage
                                r_stage.input_para[inputid-1][0] = inputsrc
                            else:
                                for index in range(len(r_stage.input_para)):
                                    stagename, usage = r_stage.input_para[index][0].split('.')
                                    if stagename in stage_index:
                                        r_stage.input_para[index][0] = stagename+"_Iter_"+str(i+1)+"."+usage
                        rlist.append(r_stage)
        self.stagelist = rlist                


    # --------------------------------------------------------------------------
    #
    def generate_tasks(self):

        #initial task generation
        for stage in self.stagelist:
            for i in range(stage.num_tasks):
                task = Task(stage.name+"_"+str(i), stage.task_type, stage.processes, 0, 
                            stage.read_buf, stage.write_buf, [], [], 
                            stage.interleave_option, stage.mode)
                stage.task_list.append(task)
            #print(len(stage.task_list))

        #generate inputs and outpus    
        for stage in self.stagelist:
            #the order of input, length, and output is functional dependency
            #length can be a function of input size, 
            #output size can be a function of input size or task length

            tpointer = 0
            #now generate input files
            for i in range(len(stage.input_para)):
                source = stage.input_para[i][0]
                parameter = stage.input_para[i][1]
                if source == "filesystem":
                    sizel = self.generate_function(parameter[0], parameter[1], stage.num_tasks)
                    for t in range(stage.num_tasks):
                        fname = stage.name+"_Input_"+str(i)+"_"+str(t+1)
                        fsize = sizel[t]
                        f = File(fname, fsize)
                        stage.task_list[t].inputlist.append(f)
                else:
                    #handle reference input files
                    if source != "":
                        stage_name, usage_id = source.split('.')
                        usage, idt = usage_id.split('_')
                        idd = int(idt)-1
                        flist = []
                        for s in self.stagelist:
                            if s.name == stage_name:
                                if usage == "Input":
                                    for t in s.task_list:
                                        flist.append(t.inputlist[idd])
                                elif usage == "Output":
                                    for t in s.task_list:
                                        flist.append(t.outputlist[idd])
                        for t in range(stage.num_tasks):
                            stage.task_list[t].inputlist.append(flist[t*len(stage.input_para)+tpointer])
                        tpointer = tpointer+1
                    tpointer = 0    
            
            #overwrite the task file mapping here
            #print(stage.input_task_mapping)
            if len(stage.input_task_mapping) > 0:
                function = stage.input_task_mapping[0]
                if function == "combination":
                    source, mt = stage.input_task_mapping[1].split()
                    m = int(mt)
                    stage_name, usage_id = source.split('.')
                    usage, idt = usage_id.split('_')
                    idd = int(idt)-1
                    flist = []
                    for s in self.stagelist:
                        if s.name == stage_name:
                            if usage == "Input":
                                for t in s.task_list:
                                    flist.append(t.inputlist[idd])
                            elif usage == "Output":
                                for t in s.task_list:
                                    flist.append(t.outputlist[idd])
                    fplist = self.combination(flist, m)
                    #print(len(fplist))
                    for t in range(stage.num_tasks):
                        stage.task_list[t].inputlist = fplist[t%len(fplist)]

                elif function == "external":
                    script = stage.input_task_mapping[1]
                    cmd = os.path.abspath(script)
                    result = subprocess.getstatusoutput(cmd)
                    ret = int(result[0])
                    value = result[1]
                    if ret != 0:
                        print("execution error: "+str(value))
                        sys.exit()
                    else:
                        flist = []
                        inputgroup = value.split('\n')
                        for line in inputgroup:
                            pair = []
                            inputs = line.split()
                            for i in inputs:
                                words = i.split('_')
                                s = words[0]+"_"+words[1]
                                usage = words[2]

                                f = None
                                flag=False
                                for st in self.stagelist:
                                    if s == st.name:
                                        for t in st.task_list:
                                            if usage == "Input":
                                                for inf in t.inputlist:
                                                    if i == inf.name:
                                                        flag=True
                                                        pair.append(inf)
                                            elif usage == "Output":
                                                for outf in t.outputlist:
                                                    if i == outf.name:
                                                        flag=True
                                                        pair.append(outf)
                                if flag == False:
                                    print("ERROR: File "+i+" declared in script "+script+" does not exist")
                                    sys.exit()
                            flist.append(pair)
                        for t in range(stage.num_tasks):
                            stage.task_list[t].inputlist = flist[t]
            
            #now generate task length
            if stage.length_para[0] == "polynomial":
                m=re.match(r"(?P<para>\[\d+[\.\d+]*,\s*\d+[\.\d+]*]\])\s*(?P<src>\w*)", stage.length_para[1])
                para = m.group('para')
                src = m.group('src')
                index = int(src[6:])
                input_list = []
                for t in stage.task_list:
                    #for inf in t.inputlist:
                    input_list.append(t.inputlist[index-1].size)
                sizel = self.generate_polynomial(stage.length_para[0], stage.length_para[1], input_list)
            else:    
                sizel = self.generate_function(stage.length_para[0], stage.length_para[1], stage.num_tasks)            
            for t in range(stage.num_tasks):
                stage.task_list[t].length = sizel[t]

            #now generate output files
            for o in range(len(stage.output_para)):
                dist = stage.output_para[o][0]
                parameter = stage.output_para[o][1]
                if dist == "polynomial":
                    input_list = []
                    m=re.match(r"(?P<para>\[\d+,\s*\d+\])\s*(?P<src>\w*)", parameter)
                    para = m.group('para')
                    src = m.group('src')
                    if src[:5] == "Input":
                        index = int(src[6:])
                        for t in stage.task_list:
                            input_list.append(t.inputlist[index-1].size)
                    elif src[:6] == "Length":
                        for t in stage.task_list:
                            input_list.append(t.length)
                    sizel = self.generate_polynomial(dist, para, input_list)
                else:
                    sizel = self.generate_function(dist, parameter, stage.num_tasks)
                for t in range(stage.num_tasks):
                    fname = stage.name+"_Output_"+str(o)+"_"+str(t+1)
                    fsize = sizel[t]
                    f = File(fname, fsize)
                    stage.task_list[t].outputlist.append(f)


    # --------------------------------------------------------------------------
    #
    def combination(self, flist, m):

        import itertools
        return list (itertools.combinations (flist, m))


    # --------------------------------------------------------------------------
    #
    def generate(self, mode):

        self.mode = mode.lower ()

        self.parse()
        self.resolve_iteration()
        self.generate_tasks()
        self.setdir()


    # --------------------------------------------------------------------------
    #
    def generate_polynomial(self, distribution, parameter, input_list):

        lengthl = []
        m = re.match(r"\[(?P<a>\d+), (?P<b>\d+)\](?P<source>\w*)", parameter)
        a = float(m.group('a'))
        b = float(m.group('b'))
        for i in input_list:
            lengthl.append(str(int(a*pow(float(i), b))))
        return lengthl
            
    # --------------------------------------------------------------------------
    #
    def generate_function(self, distribution, parameter, num_items):
        lengthl = []
        if distribution == "uniform":
            lengthl = [parameter]*num_items
        elif distribution == "normal":
            m=re.match(r"\[(?P<avg>\d+|\d+.\d+), (?P<stdev>\d+|\d+.\d+)\](?P<unit>\w*)", parameter)
            avg = float(m.group('avg'))
            stdev = float(m.group('stdev'))
            unit = m.group('unit')
            randoml = []
            random.seed()
            for i in range(int(num_items)):
                randoml.append(max(0.0,random.normalvariate(avg, stdev)))
            lengthl = []
            for i in randoml:
                lengthl.append(str("%.2f" % float(i))+unit)
        elif distribution == "gauss":
            m=re.match(r"\[(?P<avg>\d+|\d+.\d+), (?P<stdev>\d+|\d+.\d+)\](?P<unit>\w*)", parameter)
            avg = float(m.group('avg'))
            stdev = float(m.group('stdev'))
            unit = m.group('unit')
            randoml = []
            random.seed()
            for i in range(int(num_items)):
                randoml.append(max(0.0,random.gauss(avg, stdev)))
            lengthl = []
            for i in randoml:
                lengthl.append(str("%.2f" % float(i))+unit)
        elif distribution == "lognorm":
            m=re.match(r"\[(?P<avg>\d+|\d+.\d+), (?P<stdev>\d+|\d+.\d+)\](?P<unit>\w*)", parameter)
            avg = float(m.group('avg'))
            stdev = float(m.group('stdev'))
            #size = int(m.group('size'))
            unit = m.group('unit')
            randoml = []
            random.seed()
            for i in range(int(num_items)):
                randoml.append(max(0.0,random.lognormvariate(avg, stdev)))
            lengthl = []
            for i in randoml:
                lengthl.append(str("%.2f" % float(i))+unit)
        elif distribution == "triangular":
            m=re.match(r"\[(?P<low>\d+|\d+.\d+), (?P<high>\d+|\d+.\d+)\](?P<unit>\w*)", parameter)
            low = float(m.group('low'))
            high = float(m.group('high'))
            #size = int(m.group('size'))
            unit = m.group('unit')
            randoml = []
            random.seed()
            for i in range(int(num_items)):
                randoml.append(max(0.0,random.triangular(low, high)))
            lengthl = []
            for i in randoml:
                lengthl.append(str("%.2f" % float(i))+unit)
        else:
            print("ERROR: unknown distribution: "+distribution)
        return lengthl

    # --------------------------------------------------------------------------
    #
    def parse(self):    
        '''
        this parse() function reads the application configuration file,
        parses the parameters for each stage,
        adds stages to self.stage list.
        '''
        lines = []
        fd = open(self.input_file, 'r')
        while True:
            line = fd.readline()
            if not line:
                break
            line=line.strip('\n').strip()
            if re.match(r"#\s*\w+", line) or re.match(r"^\s*$", line):
                #skip comments
                pass
            else:
                lines.append(line)
        fd.close()

        '''
        parse the configuration file with regular expressions
        '''
        lines.reverse()
        line = lines.pop()

        m=re.match(r"Num_Stage\s*=\s*(?P<size>\d+)", line)
        size = m.group('size')
        self.num_stage = int(size)

        for i in range(self.num_stage):
            iteration_num = 1
            iteration_stages = ""
            iteration_sub = []
            input_para = []
            output_para = []
            input_task_mapping = ""

            line = lines.pop()
            m=re.match(r"Stage_Name\s*=\s*(?P<stage_name>\w+)", line)
            stage_name = m.group('stage_name')
            #print("stage_name: %s" % stage_name)

            line = lines.pop()
            m=re.match(r"Task_Type\s*=\s*(?P<task_type>.+)", line)
            task_type = m.group('task_type')    
            #print("task_type: %s" % task_type)

            line = lines.pop()
            m=re.match(r"Num_Tasks\s*=\s*(?P<num_tasks>\d+)", line)
            num_tasks = m.group('num_tasks')    
            #print("num_tasks: %s" % num_tasks)

            line = lines.pop()
            m=re.match(r"Task_Length\s*=\s*(?P<distribution>\w+) (?P<parameter>.+)", line)
            distribution = m.group('distribution')
            parameter = m.group('parameter')
            lengthpara = [distribution, parameter]
            #print("Task_Length: "+distribution+", "+parameter)

            line = lines.pop()
            m=re.match(r"Num_Processes\s*=\s*(?P<num_processes>\d+)", line)
            num_processes = m.group('num_processes')    
            #print("num_processes: %s" % num_processes)

            line = lines.pop()
            m=re.match(r"Read_Buffer\s*=\s*(?P<read_buf>\d+)", line)
            read_buf = m.group('read_buf')    
            #print("read_buf: %s" % read_buf)

            line = lines.pop()
            m=re.match(r"Write_Buffer\s*=\s*(?P<write_buf>\d+)", line)
            write_buf = m.group('write_buf')    
            #print("write_buf: %s" % write_buf)

            line = lines.pop()
            m=re.match(r"Input_Files_Each_Task\s*=\s*(?P<input_file_each_task>\d+)", line)
            input_file_each_task = m.group('input_file_each_task')
            #print("Input_Files_Each_Task: %s" % input_file_each_task)
                    
            for i in range(int(input_file_each_task)):
                line = lines.pop()
                input_source = ""
                size = []
                if re.match(r"Input_"+str(i+1)+".Source\s*=\s*(?P<input_source>.+)", line):
                    m=re.match(r"Input_"+str(i+1)+".Source\s*=\s*(?P<input_source>.+)", line)
                    input_source = m.group("input_source")
                    #print("Input_source: "+input_source)
                else:
                    lines.append(line)

                line = lines.pop()    
                if re.match(r"Input_"+str(i+1)+".Size\s*=\s*(?P<distribution>\w+) (?P<parameter>.+)", line):
                    m=re.match(r"Input_"+str(i+1)+".Size\s*=\s*(?P<distribution>\w+) (?P<parameter>.+)", line)
                    distribution = m.group('distribution')
                    parameter = m.group('parameter')
                    size = [distribution, parameter]
                    #print("Input_Distribution: "+distribution+", "+parameter)
                else:
                    lines.append(line)
                input_para.append([input_source, size])
                
            line=lines.pop()
            if re.match(r"Input_Task_Mapping\s*=\s*(?P<distribution>\w+) (?P<input_source>.+)", line):
                m=re.match(r"Input_Task_Mapping\s*=\s*(?P<distribution>\w+) (?P<input_source>.+)", line)
                distribution = m.group('distribution')
                input_source = m.group('input_source')
                #parameter = m.group('parameter')
                input_task_mapping = [distribution, input_source]
                #print("Input_Task_Mapping: "+distribution+", "+input_source+", "+parameter)
                #if len(input_para) > 0:
                #    for i in input_para:
                #        if i[0] == "":
                #            i[0] = input_source
            else:
                lines.append(line)


            line = lines.pop()
            m=re.match(r"Output_Files_Each_Task\s*=\s*(?P<output_file_each_task>\d+)", line)  
            output_file_each_task = m.group('output_file_each_task')
            #print("Output_Files_Each_Task: %s" % output_file_each_task)

            for i in range(int(output_file_each_task)):
                line = lines.pop()
                m=re.match(r"Output_"+str(i+1)+".Size\s*=\s*(?P<distribution>\w+) (?P<parameter>.+)", line)
                distribution = m.group('distribution')
                parameter = m.group('parameter')
                output_para.append([distribution, parameter])
                #print("Output_Distribution: "+distribution+": "+parameter)

            line = lines.pop()
            m=re.match(r"Interleave_Option\s*=\s*(?P<interleave_option>\d+)", line)
            interleave_option = m.group('interleave_option')    
            #print("interleave_option: %s" % interleave_option)

            if len(lines) > 0:
                line = lines.pop()
                if re.match(r"Iteration_Num\s*=\s*(?P<iteration_num>\d+)", line):
                    m=re.match(r"Iteration_Num\s*=\s*(?P<iteration_num>\d+)", line)
                    iteration_num = m.group('iteration_num')
                    #print("iteration_num: "+iteration_num)

                    line = lines.pop()
                    m=re.match(r"Iteration_Stages\s*=\s*(?P<iteration_stages>.+)", line)
                    iteration_stages = m.group('iteration_stages')
                    #print("iteration_stages: "+iteration_stages)
                    
                    line = lines.pop()
                    m=re.match(r"Iteration_Substitute\s*=\s*(?P<src>.+)\s*,\s*(?P<tgt>.+)", line)
                    src = m.group('src')
                    tgt = m.group('tgt')
                    iteration_sub = [src, tgt]
                    #print("Iteration_Substitute: "+ src+"    "+tgt)
                else:
                    lines.append(line)

            stage = Stage(stage_name, task_type, num_tasks, lengthpara,
                          num_processes, read_buf, write_buf, 
                          input_file_each_task,  input_para,  input_task_mapping, 
                          output_file_each_task, output_para, interleave_option, 
                          iteration_num, iteration_stages, iteration_sub, self.mode)
            self.stagelist.append(stage)



    # --------------------------------------------------------------------------
    #
    def as_shell (self) :

        s = ""
        for stage in self.stagelist:
            for t in stage.task_list:
                s += "%s\n" % t.get_command (stage)
        return s    


    # --------------------------------------------------------------------------
    #
    def as_dax (self) :

        try :
            import Pegasus.DAX3
        except ImportError as e :
            print "WARNING: cannot import Pegasus"
            sys.exit (0)

        for stage in self.stagelist:
            for t in stage.task_list:
                t.get_args()

        filemap = dict()
        taskmap = dict()
        dax = Pegasus.DAX3.ADAG(self.name)
        for stage in self.stagelist:
            for para in stage.input_para:
                if para[0] == "filesystem":
                    index = stage.input_para.index(para)
                    for t in stage.task_list:
                        for f in t.inputlist:
                            i = 0
                            if f.name not in filemap:
                                path = os.path.join(stage.inputdir[i], f.name)
                                a = Pegasus.DAX3.File(path)
                                a.addPFN(Pegasus.DAX3.PFN("file://" + os.getcwd() + "/" + path, "local"))
                                dax.addFile(a)
                                filemap[f.name] = 1
                                i = i+1
        exe = "./task"                        
        exe_stage = Pegasus.DAX3.Executable(namespace=self.name, name=stage.name, version="1.0", os="linux", arch="x86_64", installed=False)
        exe_stage.addPFN(Pegasus.DAX3.PFN("file://" + os.getcwd() + "/" + exe, "local"))

        infilemap = dict()
        outfilemap = dict()
        for stage in self.stagelist:
            for task in stage.task_list:
                t = Pegasus.DAX3.Job(namespace=self.name, name=task.taskid, version="1.0")
                t.addArguments(task.args)
                i = 0
                for f in task.inputlist:
                    if f.name not in infilemap:
                        path = os.path.join(stage.inputdir[i], f.name)
                        fh = Pegasus.DAX3.File(path)
                        t.uses(fh, link=Pegasus.DAX3.Link.INPUT)
                        infilemap[path] = fh
                        i = i+1
                    else:
                        t.uses(infilemap[f.name], link=Pegasus.DAX3.Link.INPUT)
                i = 0
                for f in task.outputlist:
                    if f.name not in outfilemap:
                        path = os.path.join(stage.outputdir[i], f.name)
                        fh = Pegasus.DAX3.File(path)
                        t.uses(fh, link=Pegasus.DAX3.Link.OUTPUT)
                        outfilemap[path] = fh
                        i = i+1
                    else:
                        t.uses(outfilemap[path], link=Pegasus.DAX3.Link.OUTPUT)
                taskmap[task.taskid] = t
                dax.addJob(t)
        
        for stage in self.stagelist:
            for index in range(len(stage.input_para)):
                para = stage.input_para[index]
                if para[0] != "filesystem":
                    for t in stage.task_list:
                        f = t.inputlist[index]
                        words = f.name.split('_')
                        if len(words) > 5:
                            stage_name = words[0]+'_'+words[1]+'_'+words[2]+'_'+words[3]
                        else:
                            stage_name = words[0]+'_'+words[1]
                        #print(stage_name)
                        for st in self.stagelist:
                            if st.name == stage_name:
                                for tt in st.task_list:
                                    for g in tt.outputlist:
                                        if f.name == g.name:
                                            dax.addDependency(Pegasus.DAX3.Dependency(parent=taskmap[tt.taskid], child=taskmap[t.taskid]))

        return dax.toXML ()


    # --------------------------------------------------------------------------
    #
    def as_swift (self) :

        s = "type file;"+"\n"

        for stage in self.stagelist:
            s += "app ("
            for i in range(int(stage.output_per_task)):
                s += "file output_"+str(i)+", "
            if s == "app (":
                s += ' '
            else:
                s = s[:len(s)-2]
            s += ') '
            s += stage.name+'('
            if int(stage.input_per_task) > 0:
                for i in range(int(stage.input_per_task)):
                    s += "file input_"+str(i)+", "
            s += 'string t_type, int procs, float length, int read_buf, int write_buf, int num_in, int num_out, int inter_opt, '
            if int(stage.output_per_task) > 0:
                for i in range(int(stage.output_per_task)):
                    s += "string outsize_"+str(i)+", "
            if s[len(s)-2:] == ", ":
                s = s[:len(s)-2]
            s += ') {'+'\n'
            s += '    '+'task t_type procs length read_buf write_buf num_in num_out inter_opt'+' '
            if int(stage.input_per_task) > 0:
                for i in range(int(stage.input_per_task)):
                    s += "@input_"+str(i)+" "
            if int(stage.output_per_task) > 0:
                for i in range(int(stage.output_per_task)):
                    s += "@output_"+str(i)+" "+'outsize_'+str(i)+' '
            s = s.strip()+";\n}\n"
            s += '\n'

        for stage in self.stagelist:
            s += stage.name+"_proc"+"(){\n"
            s += '    float '+stage.name+'_length[] = ['
            for task in stage.task_list:
                s += str(float(task.length.strip('s'))) + ', '
            if s[len(s)-2:] == ', ':
                s = s[:len(s)-2]
            s += '];\n'

            s += '    string '+stage.name+'_inputfiles[] = ['
            for t in stage.task_list:
                i = 0
                for f in t.inputlist:
                    inputdir = stage.inputdir[i]
                    filepath = os.path.join(inputdir, f.name)
                    s += '\"'+filepath+ '\", '
                    i = i+1
            if s[len(s)-2:] == ', ':
                s = s[:len(s)-2]
            s += '];\n'
            
            s += '    string '+stage.name+'_outputfiles[] = ['
            for t in stage.task_list:
                i = 0
                for o in t.outputlist:
                    outputdir = stage.outputdir[i]
                    filepath = os.path.join(outputdir, o.name)
                    s += '\"'+filepath+ '\", '
                    i = i+1
            if s[len(s)-2:] == ', ':
                s = s[:len(s)-2]
            s += '];\n\n'

            s += '    string '+stage.name+'_outputsizes[] = ['
            for t in stage.task_list:
                for o in t.outputlist:
                    s += '\"'+o.size.strip('B')+'\"' + ', '
            if s[len(s)-2:] == ', ':
                s = s[:len(s)-2]
            s += '];\n\n'

            s += '    foreach l, i in '+stage.name+'_length{\n'
            for i in range(int(stage.input_per_task)):
                s += "        file "+stage.name+"_infile_"+str(i)+" <single_file_mapper; file="+stage.name+"_inputfiles[i*"+str(stage.input_per_task)+"+"+str(i)+"]"+">;\n"
                
            for i in range(int(stage.output_per_task)):
                s += "        file "+stage.name+"_outfile_"+str(i)+" <single_file_mapper; file="+stage.name+"_outputfiles[i*"+str(stage.output_per_task)+"+"+str(i)+"]"+">;\n"

            if int(stage.output_per_task) > 0:
                for i in range(int(stage.output_per_task)):
                    s += "        string outsize_"+str(i)+" = "+stage.name+"_outputsizes[i*"+str(stage.output_per_task)+"+"+str(i)+"];"    
            s += '\n'
            s += "        string t_type = \""+stage.task_type+"\";\n"
            s += "        int procs = "+str(stage.processes)+";\n"
            s += "        int read_buf = "+stage.read_buf+";\n"
            s += "        int write_buf = "+stage.write_buf+";\n"
            s += "        int num_in = "+str(stage.input_per_task)+";\n"
            s += "        int num_out = "+str(stage.output_per_task)+";\n"
            s += "        int inter_opt = "+ stage.interleave_option+";\n"
            s += '\n'

            if int(stage.output_per_task) > 0:
                for i in range(int(stage.output_per_task)):
                    s += "        "+stage.name+"_outfile_"+str(i)+", "
                if s[len(s)-2:] == ", ":
                    s = s[:len(s)-2]                        
            s += " = "
            s += stage.name + '('
            if int(stage.input_per_task) > 0:
                for i in range(int(stage.input_per_task)):
                    s += stage.name+"_infile_"+str(i)+", "
            s += 't_type, procs, l, read_buf, write_buf, num_in, num_out, inter_opt, '
            if int(stage.output_per_task) > 0:
                for i in range(int(stage.output_per_task)):
                    s += " outsize_"+str(i) + ", " 

            if s[len(s)-2:] == ", ":
                s = s[:len(s)-2]
            s += ');\n'
            s += '    }\n'+'}\n\n'

        s += 'iterate i {\n'
        s += '    switch(i){\n'
        i = 0
        for stage in self.stagelist:
            s += '        case '+str(i)+':\n'
            s += '            '+stage.name+'_proc();\n'
            i = i+1
        s += '    }\n'
        s += '}until(i=='+str(i)+');\n'

        return s


    # --------------------------------------------------------------------------
    #
    def as_json (self) :

        # make sure all tasks have self._command defined
        for stage in self.stagelist :
            for task in stage.task_list :
                task.get_command (stage)

        import json

        def convert_to_builtin_type(obj):
            # print 'default(', repr(obj), ')'
            # Convert objects to a dictionary of their representation
            d = { 'class':obj.__class__.__name__, 
                  # '__class__':obj.__class__.__name__, 
                  # '__module__':obj.__module__,
                  }
            d.update(obj.__dict__)
            return d

        return json.dumps(self, default=convert_to_builtin_type, 
                          indent=4, separators=(',', ' : ')) + "\n"

#
# ------------------------------------------------------------------------------

