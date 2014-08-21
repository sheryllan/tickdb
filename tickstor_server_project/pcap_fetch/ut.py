# Helper functions, logging, etc...
# vim: expandtab ts=4 ai

__version__ = (0,0,2)

import subprocess as sp
from time import time,sleep
import sys, logging, os
from config import LOGFILE
import multiprocessing as mp

class massZip:
    def __init__(self,logginginstance):
        self.errorQ = mp.Queue()
        self.out = logginginstance
    
    def bz(self,x,cmd):
        self.out.pout("Proc Spawn >>> " + x)
        rc = os.system("/usr/bin/bunzip2 %s" % x)
        if rc != 0:
            self.out.perr("Could not unzip file: %s, got return code %s" % (x,rc))
            self.errorQ.put(x)

    def execute(self,directory,ziptype):
        ''' does what it says on the tin. Given a dir it walks it and bunzips it all in parallel '''
        if ziptype == "bunzip":
            files = os.popen("find %s -type f -name \"*.bz2\"" % directory).readlines()
            self.out.pout("We are batch bunzipping %d files" % len(files) )
            cmd = "/usr/bin/bunzip2" 
        elif ziptype == "bzip":
            files = os.popen("find %s -type f -not -name \"*.bz2\"" % path).readlines()
            self.out.pout("We are batch bunzipping %d files" % len(files))
            cmd = "/usr/bin/bzip2"
        else:
            self.out.perr("Sorry, type '%s' unrecognized, failing" % ziptype)
            raise AttributeError("ziptype '%s' not recognised" % ziptype)

        files = map( lambda x: x.strip(), files)

        failures = []
        running_procs = []

        while True:
            if len(files) == 0: break
            while (len(running_procs ) < mp.cpu_count() ):
                try: p = mp.Process(target=self.bz, args=(files.pop(),cmd))
                except IndexError: break
                p.start()
                running_procs.append(p)

            sleep(0.5)
            running_procs = filter(lambda x: x.is_alive() == True, running_procs) #cleanup
            if len(running_procs) == 0: break

        while self.errorQ.empty() == False:
            failures.append(self.errorQ.get())
        #serial write to log file (expensive, due to mass syscalls, but we are not caring here)
        if len(failures) != 0:
            map(lambda x: self.out.perr(x), failures)
        return failures

    def bunzipdir(self,x):
        return self.execute(x,"bunzip")

    def bzipdir(self,directory):
        return self.execute(x,"bzip")


class output:
    def __init__(self,logfile=None,loglevel=logging.DEBUG):
        self.logfile = logfile
        if logfile != None:
            logging.basicConfig(filename=LOGFILE, level=loglevel)

    def pout(self,text):
        ''' Write text to stdout. Use instead of print (as print is a function in python3)  '''     
		text = "%s: %s \n" % (time(),text)
        sys.stdout.write(text)
        logging.info(text)

    def pwarn(self,text):
		text = "%s:WARNING: %s \n" % (time(),text)
        sys.stderr.write(text)
        logging.warning(text)

    def perr(self,text):
        ''' Write text to stderr, if log != None, append to log. Not multiprocess safe! '''
		text = "%s:ERROR: %s \n" % (time(),text)
        sys.stderr.write(text)
        logging.error(text)


   #     if log != None:
    #        fd = open(log,"a")
     #       fd.write ("%s: %s \n" % (time(),text))
      #      fd.close()

def call(cmd, *args):
    # Execute command specified, and return stdout
    cmd = [cmd]
    cmd.extend(args)
    pipe = sp.Popen(  cmd ,  shell=False, bufsize=65535, stdout=sp.PIPE, close_fds=True)
    return pipe.stdout.readlines()

def system(cmd, *args):
    cmd = [cmd]
    cmd.extend(args)
    return sp.call( cmd, shell=False )

class flexidict(dict):
    def __init__(self,*args):
        dict.__init__(self,args)
    def __setitem__(self,key,val):
        try:dict.__setitem__(self,key,val)
        except KeyError:dict.update(self,{key:value})

class pflexidict(flexidict):
    def __init__(self,*args):
        flexidict.__init__(self,*args)
    def __setitem__(self,key,val):
        try:
            if type( flexidict.__getitem__(self,key)) == list:
                items = flexidict.__getitem__(self,key)
                items.append(val)
                val = items
            else:
                val = []
            flexidict.__setitem__(self,key,val)
        except KeyError:
            flexidict.__setitem__(self,key,[val])
            
            

