#!/usr/bin/env python

##############################################################################
#
#	FILE: listener.py
#	DATE: 2013 Nov. 12
#	AUTHOR: Brendon Walter
#
#	DESCTIPTION: listener.py uses psutil to listen to the CPU, memory, 
#	network information, and disk space and logs it to a file called 
#	listenerLog.log. It also checks the current directory to see what files
#	are currently in it and checks it against a list of files that need to
#	be there, as stated in the listernerConfig.py. These files that need
#	to be here should be in the form of a list. It should look like as 
# 	follows: files = ["hosts.txt", "config.py"]. 
#	If either of those items are not found in the current directoy, it will
#	return a critical error that should be checked as soon as possible.
#
#	USAGE: run the listener by:
#		python listener.py <-v> <run delay>
#	or:
#		./listener.py <-v> <run delay>
#	running the listener in verbose mode (-v) will print out information
#	for debugging purposes. The run delay (in seconds) will determine how
#	often the program runs through the main loop and thus, how often it 
#	checks the CPU, memory, network, and disk usage.
#
###############################################################################

import psutil, time, sys, logging, os, listenerConfig

###########################

#	L O G G I N G	  #

###########################

# Setup for having a verbose mode for debugging:
# USAGE: When running program, $python chunkserver.py , no debug message will
# show up. Instead, the program should be run in verbose, $
#	python chunkserver.py -v ,
# for debug messages to show up

# Get a list of command line arguments
args = sys.argv
# Check to see if the verbose flag was one of the command line arguments
if "-v" in args:
        # If it was one of the arguments, set the logging level to debug
        logging.basicConfig(level=logging.DEBUG, format='%(levelname)s : %(message)s')
else:
        # If it was not, set the logging level to default (only shows messages
        # with level warning or higher)
        logging.basicConfig(filename='httpServerFiles/listenerErrors.log', format='%(asctime)s %(levelname)s : %(message)s')

#################################

# 	V A R I A B L E S	#

#################################

# The time that the program waits before running again is given in an argument.
try:
	# The first argument is the delay time
	delayTime = float(args[1])
	logging.debug("the time delay is " + str(delayTime))
except ValueError:
	# if there is a value error (as in, -v is put before the dealytime,
	# it will assume that the second argument in the list is the delaytime.
	try:
		delayTime = float(args[2])
		logging.debug("the time delay is " + str(delayTime))
	except IndexError as e:
		logging.debug("user forgot to put in the time delay")
		print e
		print "Please put in the time delay as an argument."
		exit()
except IndexError as e:
	logging.debug("user forgot to put in the time delay")
	print e
        print "Please put in the time delay as an argument."
        exit()

# name of the log in which all of the (non-error related) information
# is stored.
logName = listenerConfig.logName
logging.debug("Name of listen log: " + logName)

# "files" is a list of files that need to be in the directory. 
files = listenerConfig.files

for x in files: logging.debug("Files in config: " + x)

#################################

# 	F U N C T I O N S	#

#################################

def logInfo(kind, info):
	# puts all of the information into a log
	logging.debug(kind + ": " + str(info))
	with open(logName, 'a') as f:
		f.write(kind + ': ' + str(info) + '\n')

def getCPU():
	# gets the percent of the CPU in use
	cpuPercent = psutil.cpu_percent()
	logInfo("CPU", cpuPercent)

def getMemory():
	# gets the total, used, free, and percentage of both virtual and 
	# swap memory
	virtualMemory = psutil.virtual_memory()
	logInfo("MEMORY", virtualMemory)
	swap = psutil.swap_memory()
	logInfo("MEMORY", swap)

def getNetwork():
	# gets bytes send, bytes recieved plus other information about the
	# network.
	stats = psutil.network_io_counters()
	logInfo("NETWORK", stats)

def getDisk():
	# recieves information about the disk
	space = psutil.disk_usage('/')
	logInfo("DISK", space)

def filesMissing():
	# reads the local directory  and checks to make sure that the files
	# that are supposed to be there are there

	# put the name of all the files present in the directory into a list
	# called currentFiles created below
	currentFiles = []
	for current in os.listdir('.'):
		currentFiles.append(current)
	logging.debug("The current files in the directory are: " + str(currentFiles))
	# for debugging purposes
	for item in files:
		logging.debug("The files that should be here are: " + str(item))


	# check to see if there is an item in files that is not in current
	if all(item in currentFiles for item in files): isThere = False
        else: isThere = True

	# if a file is missing
	if isThere == True:
		# log a critical error. This needs to be checked!!
		logging.critical("File missing from directory!!")
	else: 
		# otherwise, continue as normal.
		logging.debug("Every file that's supposed to be here is here.")
	

#################################

#	M A I N   C O D E	#

#################################

if __name__ == '__main__':
	while 1:
		time.sleep(delayTime)
		getCPU()
		getMemory()
		getNetwork()
		getDisk()
		filesMissing()
