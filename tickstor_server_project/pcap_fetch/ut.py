# Useful tools library. for functions which I re-use often in different
# projects
# vim: expandtab ts=4 ai

__version__ = (0,0,1)

import subprocess as sp

def pout(text):
    ''' Write text to stdout. Use instead of print (as print is a function in python3)  '''     
    print text

def perr(text,log=None):
    ''' Write text to stderr, if log != None, append to log. Not multiprocess safe! '''
    sys.sterr.write(text +"\n")
    if log != None:
        fd = open(log,"a")
        fd.write (text + "\n")
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



