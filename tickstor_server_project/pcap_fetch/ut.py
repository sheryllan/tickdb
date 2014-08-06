# Useful tools library. for functions which I re-use often in different
# projects
# vim: expandtab ts=4 ai

__version__ = (0,0,1)

import subprocess as sp
from time import time
import sys

def pout(text):
    ''' Write text to stdout. Use instead of print (as print is a function in python3)  '''     
    sys.stdout.write("%s: %s \n" % (time(),text) )

def perr(text,log=None):
    ''' Write text to stderr, if log != None, append to log. Not multiprocess safe! '''
    sys.stderr.write("%s: %s \n" % (time(),text) )
    if log != None:
        fd = open(log,"a")
        fd.write ("%s: %s \n" % (time(),text))
        fd.close()

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
            
            

