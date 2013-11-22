#!/usr/bin/env python

###############################################################################
#
#
#       FILE: listener.py
#       DATE: 2013 Nov. 15
#       AUTHOR: Brendon Walter
#
#       DESCTIPTION: listener.py uses psutil to listen to the CPU, memory, 
#       network information, and disk space and logs it to a file called 
#       listenerLog.log. It also checks the current directory to see what files
#       are currently in it and checks it against a list of files that need to
#       be there, as stated in the listernerConfig.py. These files that need
#       to be here should be in the form of a list. It should look like as 
#       follows: files = ["hosts.txt", "config.py"]. 
#       If either of those items are not found in the current directoy, it will
#       return a critical error that should be checked as soon as possible.
#
#       USAGE: run the listener by:
#               python listener.py <-v>
#       or:
#               ./listener.py <-v>
#       running the listener in verbose mode (-v) will print out information
#       for debugging purposes. 
#
#	The listenerConfig contains the name of the log that will store all 
#	errors, the number of items per line, which will determine how many
#	items are stored on each line (such as, if this number is set at 60,
#	the history of the CPU (and every part) will go back 60 items into its
#	past.) The delayTime will determine how often (in seconds) the program
#	will run through, and files is a list of every file that is supposed to
#	be in the folder in which the listener is running.
#       
#
###############################################################################

import psutil, time, sys, logging, os, re, listenerConfig
import functionLibrary as fL

# import debugging
fL.debug()

##########################

#    V A R I A B L E S   #

##########################

# The time that the program waits before running again is given in an argument.
delayTime = listenerConfig.delayTime

# name of the log in which all of the information about the system (such as the 
# CPU and memory) is stored
logName = listenerConfig.logName
logging.debug("Name of listen log: " + logName)

# name of log that contains all data stored
fullLog = listenerConfig.fullLog

# Every error is logged to a different log, the name of which is also given in 
# the configuration log.
errorLog = listenerConfig.errorLogName
logging.debug("Name of the error log: " + errorLog)

# "files" is a list of files that need to be in the directory. 
files = listenerConfig.files

for x in files: logging.debug("Files in config: " + x)

totItems = listenerConfig.numberOfItemsPerLine
logging.debug(str(totItems) + " items per line in log.")

# time stamp is used in the fullLog to make the log more useful and avaiable
# for analysis. Format for the time stamp is year.month.day.hour.minute.second
# it automates to using GM time so that in a system distrubuted across multiple
# timezones, the time stamps won't conflict.
t = time.gmtime()
stamp = str(t.tm_year) + '.' + str(t.tm_mon) + '.' + str(t.tm_mday) + '.' + str(t.tm_hour) + '.' + str(t.tm_min) + '.' + str(t.tm_sec)



###########################

#    F U N C T I O N S    #

###########################

def logError(info):
	# If you are using the listener in another program to log an error (such
	# as, if there is a file missing), use this function! If the listener is 
	# imported as listener, then this should be used as:
	# 	listener.logError("What the error is. Be descriptive!")

	with open(errorLog, 'a') as a:
		a.write(str(info) + '\n')
		logging.debug("Logged " + str(info) + " to the error log.")

def logInfo(lineNum, info):
        # logs CPU, network, disk, and memory to a file to be used for display
	# on the web

        # put the information from the listener log into a var called data
        with open(logName, 'r') as r:
                data = r.readlines()
        logging.debug("Read " + str(data) + " from the log.")

        # split the data from the line spefied  at the pipe and newline character 
        try: l = re.split('[|\n]', data[int(lineNum)])
        # if there is no data at that line, it will return an index error. If this
        # happens, it will make a list with one character in it that will then
        # be deleted
        except IndexError: l = [0]
        # the last index in the list is deleted as it is an empty string
        del l[-1]

        # if there are more items in the list than there should be as specified
        # in the config file, delete the first item
        while len(l) >= totItems: del l[0]

        # add new data to the end of the list
        l.append(str(info))
        logging.debug("Appended " + str(info) + " to the list.")

        # turn the data into a string, each part seperated by a pipe
        newData = '|'.join(l)

        try: # try to add the new data to the list of data
                # in the case that the file is empty
                if data == []: data = newData + '\n'
                # in the case that there already is information at that line
                else: data[int(lineNum)] = newData + '\n'
        # in the case that the line that should have new data added to it doesn't 
        # exist yet
        except IndexError: data.append(newData + '\n')

        # add the data to the listener log
        with open(logName, 'w') as w:
                w.writelines(data)

def logAll(lineNum, info):
        # function logs all information into one log that never gets deleted.
        # To keep the file from being completely overwhelming, every week the 
        # log will be rolled out (so listener.log becomes listener.log.1, etc).
        # Also, each bit of data includes a time stamp for it so that it is 
        # easier to follow. The format for the log is as follows:

        # line 0 CPU: <time>:<cpu percent>|<time>:<cpu percent>|...
        # line 1 Memory: <time>:<memory percent>|<time>:<memory percent>|...
        # line 2 Network: <time>:<sent>/<recv>|<time>:<sent>/<recv>|...
        # line 3 Disk: <time>:<used>/<available>|<time>:<used>/<available>|...

        # read what is currently in the log file and put it into a list
        # called data
        try:
                with open(fullLog, 'r') as r:
                        data = r.readlines()
                logging.debug("Read " + str(data) + " from the log.")
        except IOError:
                logging.debug(str(fullLog) + " does not exist!!")
                exit()

        # split the data from the line spefied  at the pipe and newline character 
        try: l = re.split('[|\n]', data[int(lineNum)])
        # if there is no data at that line, it will return an index error. If this
        # happens, it will make a list with one character in it that will then
        # be deleted
        except IndexError: l = [0]
        # the last index in the list is deleted as it is an empty string
        del l[-1]

        # add new data to the end of the list
        l.append(stamp + ':' + str(info))
        logging.debug("Appended " + str(info) + " to the list.")

        # turn the data into a string, each part seperated by a pipe
        newData = '|'.join(l)

        try: # try to add the new data to the list of data
                # in the case that the file is empty
                if data == []: data = newData + '\n'
                # in the case that there already is information at that line
                else: data[int(lineNum)] = newData + '\n'
        # in the case that the line that should have new data added to it doesn't 
        # exist yet
        except IndexError: data.append(newData + '\n')

        # add the data to the listener log
        with open(fullLog, 'w') as w:
                w.writelines(data)
	
def getCPU(lineNum):
	# gets the percent of the CPU in use

	# the line of the log in which all of the CPU information is stored
	line = lineNum
	cpuPercent = psutil.cpu_percent()
	logging.debug("CPU percent : " + str(cpuPercent))
	logInfo(line, cpuPercent)

def getMemory(lineNum):
	# gets the percent of virtual memory in use

	line = lineNum
	vm = psutil.virtual_memory()
	# pick out the percent from the psutil output
	vmPercent = vm.percent
	# log the information
	logging.debug("Virtual memory percent : " + str(vmPercent))
	logInfo(line, vmPercent)

def getNetwork(lineNum):
	# gets the number of bytes sent and recieved over the network

	line = lineNum
	network = psutil.net_io_counters()
	# get the sent information
	sent = network.bytes_sent
	# and the received information
	receieved = network.bytes_recv
	info = str(sent) + '/' + str(receieved)
	logInfo(line, info)

def getDisk(lineNum):
	# gets the space used and space available from the disk
	# the output is logged in the form of : USED / AVAILABLE

	line = lineNum
	disk = psutil.disk_usage('/')
	# get the disk used from the psutil output
	u = str(disk.used)
	# remove the last character ('L') so that the data is only numbers
	used = u[:-1]
	logging.debug("Disk used : " + str(used))
	# get the free space from the psutil output
	f = str(disk.free)
	# remove 'L'
	free = f[:-1]
	logging.debug("Disk free : " + str(free))
	info = used + '/' + free
	logInfo(line, info)

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

	missing = []
	# compare the files that are supposed to be here to files that actually
	# are here.
	for val in files:
		if val not in currentFiles:
			# if a file that is supposed to be here is not here, add
			# the name of the file to a list called missing
			missing.append(val)

	# if there is nothing in the list of missing files
	if missing == []:
		# everything is fine
		logging.debug("Every file that is supposed to be here is here.")
	else:
		for item in missing:	
			# for each item that is missing, log a critical error
#			logging.critical("File " + str(item) + " is missing!!")
			logError("FILE MISSING : " + str(item))


###########################

#    M A I N   L O O P    #

###########################

logging.debug("Starting main loop.........")

if __name__ == '__main__':
	while 1:
		getCPU(0)
		getMemory(1)
		getNetwork(2)
		getDisk(3)
		filesMissing()
		time.sleep(delayTime)
