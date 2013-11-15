#!/usr/bin/env python

import psutil, time, sys, logging, os, re, listenerConfig

#######################

#    L O G G I N G    #

#######################

# Setup for having a verbose mode for debugging:
# USAGE: When running program, $python chunkserver.py , no debug message will
# show up. Instead, the program should be run in verbose, $
#       python chunkserver.py -v ,
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

##########################

#    V A R I A B L E S   #

##########################

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

totItems = listenerConfig.numberOfItemsPerLine
logging.debug(str(totItems) + " items per line in log.")

count = [0]

###########################

#    F U N C T I O N S    #

###########################

def logInfo(lineNum, info):
	# logs info from each of the below functions into a listener log

	# try to read the listener log from the line specified
	with open(logName, 'r') as r:
		# take the data in the line and split it into a list
		data = r.readlines()
	logging.debug("read " + str(data) + " from the log.")

	if data == []:
		print "line is empty."	
		l = []

	# if the file is empty, there should be some way of handling this!!

	else: 
		# take the data from the log and split it into a list at the | and \n
		try:
			l = re.split('[|\n]', data[int(lineNum)])
		except IndexError:
			l = [0]
	
		# the list 2 items are empty strings (at least for the CPU). This will have 
		# to be removed depending on what happens with the other functions.
		del l[-1]
		
	# if the list has more elements than it should, delte them until it has the
	# right number
	while len(l) > totItems:
		del l[0]
	# if the list already has the number of items that it should,
	if len(l) == totItems:
		logging.debug("Line " + str(lineNum) + " was longer than " + str(totItems))
		# delete the first item
		del l[0]
	else: logging.debug("Line " + str(lineNum) + " was too short. Adding new item.")

	# append the new data to the list
	l.append(str(info))
	logging.debug("appended " + str(info) + " to the list.")

	# turn that list into a string
	newData = '|'.join(l)

	# add a new line character to the end of the string
	try:
		if data == []:
			data = newData + '\n'
		else: 
			data[int(lineNum)] = newData + '\n'

	except IndexError:
		data.append(newData + '\n')
	
	# write new data to the log
	with open(logName, 'w') as w:
		w.writelines(data)
	
def getCPU():
	# gets the percent of the CPU in use
	lineNum = 0 # the line of the log in which all of the CPU information is stored
	cpuPercent = psutil.cpu_percent()
	logging.debug("CPU percent : " + str(cpuPercent))
	logInfo(lineNum, cpuPercent)

def test():
	lineNum = 2
	count[0] += 1
	logging.debug("count put to " + str(count[0]))
	logInfo(lineNum, count[0])

def getMemory():
	pass

def getNetwork():
	pass

def getDisk():
	pass

# This should be looked at to make it so that it will print what files are missing
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


###########################

#    M A I N   L O O P    #

###########################

logging.debug("Starting main loop.........")

while 1:
	getCPU()
	test()
	getMemory()
	getNetwork()
	getDisk()
#	filesMissing()
	time.sleep(delayTime)
