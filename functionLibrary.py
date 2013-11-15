#!/usr/bin/env

#################################################################################
#										
#		GFS COMMON FUNCTION LIBRARY 					
#_______________________________________________________________________________
#																
# Date:		30 October 2013							
# File:		functionLibrary.py 						
#										
# Summary:	The function library is a library that all components of the GFS
#		implementation have access to, which contains basic functions that	
#		are used by multiple components, so the same code need not be 	
#		written repeatedly. 						
#										
#################################################################################


import config, sys, logging, socket, random


###############################################################################

#               DEFINE SOME CONSTANTS                                        #

###############################################################################


# From the config file, get the end of transmission character
eot = config.eot
HOSTSFILE = config.hostsfile
ACTIVEHOSTSFILE = config.activehostsfile
OPLOG = config.oplog
chunkPort = config.port



###############################################################################

#               Verbose (Debug) Handling                                      #

###############################################################################

def debug():
	# Setup for having a verbose mode for debugging:
	# USAGE: When running program, $python API.py , no debug message will show up
	# Instead, the program should be run in verbose, $python API.py -v , for debug 
	# messages to show up

	# Get a list of command line arguments
	# Check to see if the verbose flag was one of the command line arguments
	if "-v" in sys.argv:
	        # If it was one of the arguments, set the logging level to debug 
	        logging.basicConfig(level=logging.DEBUG, format='%(levelname)s : %(message)s')
	else:
	        # If it was not, set the logging level to default (only shows messages with level
	        # warning or higher)
	        logging.basicConfig(filename='apiLog.log', format='%(asctime)s %(levelname)s : %(message)s')





###############################################################################

#               TCP RECEIVE FUNCTION 

###############################################################################


def recv(connection):
	# Debug message to show the function has been called
	logging.debug('RECV: Starting recv() function')
	# Create an empty string which will eventually hold the received message
	data = ""
	# Coninuously receive data over the socket connection until the end of 
	# transmission character appears at the end of a message.
	while 1:
		# Receive the data
		d = connection.recv(1024)
		logging.debug('RECV: data has been received: ' + str(d))
		# Append the received data to the data string
		data += d
		# Create a string from the received data string which contains the last 
		# character. Creating this small string should reduce the omputational load 
		# of parsing through what could be a long string.
		try:
			ending = d[-1]
		except IndexError:
			logging.debug('RECV: No data received')
			break
		# Check the ending of the received data to see if it contains an end of
		# transmission character, and if it does, break out of the loop since 
		# no more data should be sent.
		if eot in ending:
			logging.debug('RECV: End of transmission character found. No longer receiving.')
			break

		logging.debug('RECV: End of transmission character not found. Continue receiving.')

	
	connection.send("ack " + data)
	# In the received data, remove the end of transmission character
	data = data.replace(eot, "")
	logging.debug('RECV: Data Parsed Successfully!')
	# Give back the received data
	return data



###############################################################################

#               TCP SEND FUNCTION 

###############################################################################

def send(connection, message):
	connection.send(message + eot)

	data = ""
	#then we wait on an acknowledgment
	while 1:
		# Receive the data
		d = connection.recv(1024)
		logging.debug('SEND: data has been received: ' + str(data))
		# Append the received data to the data string
		data += d
		# Create a string from the received data string which contains the last 
		# character. Creating this small string should reduce the omputational load 
		# of parsing through what could be a long string.
		try:
			ending = d[-1]
		except IndexError:
			logging.error('SEND: No data received')
			exit()
		# Check the ending of the received data to see if it contains an end of
		# transmission character, and if it does, break out of the loop since 
		# no more data should be sent.
		if eot in ending:
			logging.debug('SEND: End of transmission character found. No longer receiving.')
			break

		logging.debug('SEND: End of transmission character not found. Continue receiving.')
		
	if data == "ack " + message + eot:
		logging.debug("SEND: received ack " + data)
	else:
		logging.error("SEND: recieved incorrect message while waiting for acknowledgement: "
		 + data + "instead of " + message)




###############################################################################

#               GET AND INCREMENT CHUNK HANDLE

###############################################################################


# Function to keep track of chunkHandle numbers and to increment the number
def handleCounter():
	# Visual confirmation for debugging: confirm init of handleCounter()
	logging.debug('Generating chunk handle')
	# Create an empty string to hold the current chunk handle
	chunkHandle = ""
	# Open the text file holding the current chunkHandle number and read it into memory
	with open('handleCounter.txt', 'r') as file:
		chunkHandle = int(file.read())
	# Open the text file holding the current chunkHandle and increment it by 1
	with open('handleCounter.txt', 'w') as file:
		file.write(str(chunkHandle + 1))
	# Visual confirmation for debugging: confirm success of handleCounter()
	logging.debug('Successfully generated chunk handle')
	# Return the chunkHandle
	return chunkHandle



###############################################################################

#               RANDOMLY CHOOSE CHUNK SERVER LOCATIONS

###############################################################################


def chooseHosts():
	# Visual confirmation for debugging: confirm init of chooseHosts()
	logging.debug('Selecting storage locations')

	try:
		# Get a list of all the hosts available
		with open(ACTIVEHOSTSFILE, 'r') as file:
			hostsList = file.read().splitlines()
		# Find how many hosts there are in the list
		lengthList = len(hostsList)
		# Randomize between the limits
		randomInt = random.randint(0, lengthList)
		# Visual confirmation for debugging: confirm success of chooseHosts()
		logging.debug('Successfully selected storage locations')
		# Return a pipe-seperated list of randomized hosts
		return hostsList[randomInt%lengthList] + "|" + hostsList[(randomInt + 1)%lengthList] + "|" + hostsList[(randomInt + 2)%lengthList]

	except IOError:
		# Handle this error better in the future --> similar to how heartBeat.py
		# needs to handle for this case..
		logging.error( ACTIVEHOSTSFILE + ' does not exist')




###############################################################################

#               APPEND TO OPLOG

###############################################################################


def appendToOpLog(data):
	try:
		with open(OPLOG, 'a') as oplog:
			oplog.write(data + "\n")

	except IOError:
		logging.error("Could not append to: " + OPLOG)






