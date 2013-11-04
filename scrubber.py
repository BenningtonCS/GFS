#!/usr/bin/env python

#################################################################################
#																				#
#			GFS SCRUBBER (GARBAGE COLLECTION)									#
#_______________________________________________________________________________#
#																				#
# Authors:	Erick Daniszewski													#
# Date:		29 October 2013														#
# File:		scrubber.py 														#
#																				#
# Summary:	scrubber.py goes through the master file system database and checks	#
#			each file, and thus the chunks associated with them, to see if the 	#
#			the file/its chunks have been flagged for deletion. Once it knows 	#
#			which files/associated chunks are to be deleted, it issues delete 	#
#			commands to the chunkservers containing the condemned chunks. 		#
#																				#
#################################################################################

import socket, config, logging, sys
import database
import functionLibrary as fL


#########################################################################

#			VERBOSE (DEBUG)	HANDLING									#

#########################################################################

# Setup for having a verbose mode for debugging:
# USAGE: When running program, $python scrubber.py , no debug message will show up
# Instead, the program should be run in verbose, $python scrubber.py -v , for debug 
# messages to show up

# Get a list of command line arguments
args = sys.argv
# Check to see if the verbose flag was one of the command line arguments
if "-v" in args:
        # If it was one of the arguments, set the logging level to debug 
        logging.basicConfig(level=logging.DEBUG, format='%(levelname)s : %(message)s')
else:
        # If it was not, set the logging level to default (only shows messages with level
        # warning or higher)
        logging.basicConfig(format='%(levelname)s : %(message)s')





#########################################################################

#			SCRUBBER (GARBAGE COLLECTOR)								#

#########################################################################


class Scrubber:

	def __init__(self, data):
		self.data = data
		self.port = config.port

	def connectToCS(self, IP):
		try:
			logging.debug("Initializing connectToCS()")
			# Create a TCP socket instance
			self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			# Set the timeout of the socket
			self.s.settimeout(self.SOCK_TIMEOUT)
			# Allow the socket to re-use address
			self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			# Connect to the chunkserver over the specified port
			self.s.connect((IP, self.port))
			logging.debug("TCP Connection Successfully Established")

		except socket.error:
			logging.error("Unable to establish a connection with the chunkserver.")

	def clean(self):
		logging.debug("Commencing Clean")
		# Go through all the chunks stored in the master database
		for chunk in self.data:
			# If the chunk's delete flag is set to True
			if chunk.delete == True:
				# Get a list of all the locations where the chunk is stored in the 
				# file system
				locations = chunk.location
				# Get the chunk's identifying chunk handle
				chunkHandle = chunk.handle
				# Create a counter for successful chunk sanitizations from chunkserver
				successCount = 0
				# Send a message to all locations holding the chunk instructing it 
				# to delete the specified chunk
				for IP in locations:
					# Connect to the chunk server
					self.connectToCS(IP)
					# Send the chunk server a SANITIZE message with the chunk handle
					# so it knows which chunk it is deleting
					fL.send(self.s, 'SANITIZE|' + str(chunkHandle))
					logging.debug("Sent Sanitize Request")
					# Wait for a response back from the chunk server to verify that
					# the chunk was removed
					data = fL.recv(self.s)
					logging.debug("Received ACK")
					# If the chunk server responds with a success message, DO SOMETHING!
					if data == "SUCCESS":
						logging.debug("Chunk Successfully Deleted")
						# If the chunk was successfully deleted, increment the success counter
						successCount += 1
						
					# If the chunk server responds with a failure message, DO SOMETHING ELSE!
					elif data == "FAILED":
						# WILL NEED IMPROVED HANDLING, MAYBE A RETRY
						logging.error("Received failure message on chunk delete. Chunkhandle : " str(chunk.handle))

				logging.debug("Commence final check if delete is permissible")
				# Check to see if the number of success messages received equals the
				# number of chunkservers that were asked to sanitize.
				if successCount == len(locations):
					logging.debug("All ACKs of success, removing the chunk from the database")
					# Remove the chunk from the database
					database.data.remove(chunk)
				else:
					# WILL NEED IMPROVED ERROR HANDLING FOR THIS AND ASM PROTOCOL
					# Alert that a chunk was not able to be deleted from a chunk server
					logging.error("Not all chunks were deleted from chunkservers. Chunkhandle : " + str(chunk.handle))




#########################################################################

#			MAIN														#

#########################################################################


scrub = Scrubber(database.data)
scrub.clean()









