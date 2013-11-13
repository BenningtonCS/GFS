#!/usr/bin/env python

#################################################################################
#																	
#			GFS SCRUBBER (GARBAGE COLLECTION)					
#________________________________________________________________________________
#																
# Authors:	Erick Daniszewski									
# Date:		29 October 2013									
# File:		scrubber.py 									
#																
# Summary:	scrubber.py goes through the master file system database and checks
#		each file, and thus the chunks associated with them, to see if the 
#		the file/its chunks have been flagged for deletion. Once it knows 
#		which files/associated chunks are to be deleted, it issues delete 
#		commands to the chunkservers containing the condemned chunks. 
#																		
#################################################################################

import socket, config, logging, sys
import functionLibrary as fL


#########################################################################

#			VERBOSE (DEBUG)	HANDLING	

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

#			SCRUBBER (GARBAGE COLLECTOR)

#########################################################################


# Object that will handle the garbage collection for the GFS implementation
# by learning which files need to be deleted, informing the chunkservers which
# chunks should be deleted, and informing the database when files no longer
# exist in the system due to deletion.
class Scrubber:

	def __init__(self, data):
		logging.debug("Initilizing Scrubber Instance")
		# Get the data from the database's toDelete list
		self.data = data
		# Get the port number from the config file
		self.port = config.port
		# Set a timeout length
		self.SOCK_TIMEOUT = 3


	# Creates a TCP socket connection to the specified IP
	def connect(self, IP):
		try:
			logging.debug("Initializing connect()")
			# Create a TCP socket instance
			self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			# Set the timeout of the socket
			self.s.settimeout(self.SOCK_TIMEOUT)
			# Allow the socket to re-use address
			self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			# Connect to the chunkserver over the specified port
			self.s.connect((IP, self.port))
			logging.debug("TCP Connection Successfully Established")

		except (socket.error, socket.timeout):
			logging.error("Unable to establish a connection")


	# Inform the chunk servers of which chunks should be deleted, and upon succesful
	# deletion of all chunks in a file, inform the database that the file can be
	# removed from the database.
	def clean(self):
		logging.debug("Commencing Clean")

		# For all of the files in the toDelete list
		for file in self.data:

			self.connect(config.masterip)
			fL.send(self.s, "GETALLCHUNKS|" + file)

			data = fL.recv(self.s)
			self.s.close()

			chunkHandles = data.split("|")
			chunkHandles = filter(None, chunkHandles)
			print chunkHandles

			# Create a counter for successful chunk deletions
			successfulChunkDelete = 0
			# For each of those chunk handles
			for handle in chunkHandles:
				# Create a counter for successful deletions from a chunkserver
				successDeleteFromCS = 0

				self.connect(config.masterip)
				fL.send(self.s, "GETLOCATIONS|" + handle)

				data = fL.recv(self.s)
				self.s.close()

				# Find the locations where the chunks are being kept
				locations = data.split("|")
				locations = filter(None, locations)
				print locations

				# For each location the chunk is stored on
				for location in locations:
					# Connect to the chunk server
					self.connect(location)
					# Send the chunk server a SANITIZE message with the chunk handle
					# so it knows which chunk it is deleting
					fL.send(self.s, 'SANITIZE|' + str(handle))
					logging.debug('Sent SANITIZE message to chunkserver')
					# Wait for a response back from the chunk server to verify that
					# the chunk was removed
					data = fL.recv(self.s)
					logging.debug('Received response from chunkserver')
					
					# If the chunk server responds with a success message, DO SOMETHING!
					if data == "SUCCESS":
						logging.debug("Chunk successfully removed from chunkserver")
						successDeleteFromCS += 1
						
					# If the chunk server responds with a failure message, DO SOMETHING ELSE!
					elif data == "FAILED":
						# WILL NEED IMPROVED HANDLING, MAYBE A RETRY
						logging.error("Received failure message on chunk delete. Chunkhandle : " + str(handle))

					# If the chunk server responds with something other than SUCCESS or FAILED, something went wrong.
					else:
						logging.error("Unexpected Receive: " + str(data) + " from chunkserver " + str(location))

				# If the success counter is equal to the amount of all the IPs, then
				# all the IPs successfully deleted that chunk, so increment the 
				# successfulChunkDelete counter
				if len(locations) == successDeleteFromCS:
					successfulChunkDelete += 1
				else:
					# Improve error handling to maybe automatically retry
					logging.error("Not all chunk location deletes were successful")

			# If the number of successful chunk deletes is equal to the number of chunks
			# associated with the file, then all the chunks for that file have been deleted,
			# so the file entry can be deleted
			if len(chunkHandles) == successfulChunkDelete:
				# Call the database sanitize function, which removes the key/value pair
				# from the database.
				self.connect(config.masterip)
				fL.send(self.s, "SANITIZE|" + file)

				#data = fL.recv(self.s)
				self.s.close()
				logging.debug(str(file) + 'successfully sanitized')
				
			else:
				# Improve error handling to automatically resolve problem
				logging.error("Not all chunk deletes were successful")






#########################################################################

#			MAIN					

#########################################################################

if __name__ == "__main__":

	# Create a TCP socket instance
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	# Set the timeout of the socket
	s.settimeout(3)
	# Allow the socket to re-use address
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	# Connect to the chunkserver over the specified port
	s.connect((config.masterip, config.port))

	fL.send(s, "GETDELETEDATA|*")

	data = fL.recv(s)

	toDelete = []

	for item in data.split("|"):
		if item != "":
			toDelete.append(item)

	print toDelete

	# Create an instance of the Scrubber object, and initiate it.
	scrub = Scrubber(toDelete)
	scrub.clean()









