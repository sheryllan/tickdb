import subprocess
import os
import time

def removeFinishedProcesses(processes,log=False):
	""" given a list of (commandString, process), 
		remove those that have completed and return the result 
	"""
	newProcs = []
	for pollCmd, pollProc in processes:
		retCode = pollProc.poll()
		if retCode==None:
			# still running
			newProcs.append((pollCmd, pollProc))
		elif retCode!=0:
			# failed
			raise Exception("Command %s failed" % pollCmd)
		else:
			if log:
				logging.info("Command %s completed successfully" % pollCmd)
	return newProcs

def runCommands(commands, maxCpu,log=False,sleeptime=1):
	processes = []
	for command in commands:
		if log:
			logging.info("Starting process %s" % command)
		proc =  subprocess.Popen(shlex.split(command))
		procTuple = (command, proc)
		processes.append(procTuple)
		while len(processes) >= maxCpu:
			time.sleep(sleeptime)
			processes = removeFinishedProcesses(processes)

	# wait for all processes
	while len(processes)>0:
		time.sleep(sleeptime)
		processes = removeFinishedProcesses(processes)
	if log:
		logging.info("All processes completed")
