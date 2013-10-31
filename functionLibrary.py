#!/usr/bin/env

#################################################################################
#																				#
#			GFS COMMON FUNCTION LIBRARY 										#
#_______________________________________________________________________________#
#																				#
# Authors:	Erick Daniszewski													#
# Date:		30 October 2013														#
# File:		functionLibrary.py 													#
#																				#
# Summary:	The function library is a library that all components of the GFS	#
#			implementation have access to, which contains basic functions that	#
#			are used by multiple components, so the same code need not be 		#
#			written repeatedly. 												#
#																				#
#################################################################################


import config, sys, logging, socket



###############################################################################

#               Verbose (Debug) Handling                                      #

###############################################################################


# Setup for having a verbose mode for debugging:
# USAGE: When running program, $python functionLibrary.py , no debug message will show up
# Instead, the program should be run in verbose, $python functionLibrary.py -v , for debug 
# messages to show up

# Get a list of command line arguments
args = sys.argv
FORMAT = "%(asctime)s FUNCTION LIBRARY %(levelname)s : %(message)s"
# Check to see if the verbose flag was one of the command line arguments
if "-v" in args:
        # If it was one of the arguments, set the logging level to debug 
        logging.basicConfig(level=logging.DEBUG, format=FORMAT)
else:
        # If it was not, set the logging level to default (only shows messages with level
        # warning or higher)
        logging.basicConfig(filename='functionLibraryLog.txt', format=FORMAT)





###############################################################################

#               TCP RECEIVE FUNCTION 	                                      #

###############################################################################

# From the confid file, get the end of transmission character
eot = config.eot

def recv(connection):
	# Debug message to show the function has been called
	logging.debug('Starting recv() function')
	# Create an empty string which will eventually hold the received message
	data = ""

	# Coninuously receive data over the socket connection until the end of 
	# transmission character appears at the end of a message.
	while 1:
		# Receive the data
		d = connection.recv(1024)
		logging.debug('data has been received')
		# Append the received data to the data string
		data += d
		# Create a string from the received data string which contains the last 
		# character. Creating this small string should reduce the omputational load 
		# of parsing through what could be a long string.
		try:
			ending = d[-1]
		except IndexError:
			logging.error('Continued to recieve, but no data to receive -- no EOT character sent')
			exit()
		# Check the ending of the received data to see if it contains an end of
		# transmission character, and if it does, break out of the loop since 
		# no more data should be sent.
		if eot in ending:
			logging.debug('End of transmission character found. No longer receiving.')
			break
		logging.debug('End of transmission character not found. Continue receiving.')

	# In the received data, remove the end of transmission character
	data = data.replace(eot, "")
	logging.debug('Data Parsed Successfully!')
	# Give back the received data
	return data



