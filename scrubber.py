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

##import debugging
fL.debug()






#########################################################################

#			SCRUBBER (GARBAGE COLLECTOR)

#########################################################################


# Object that will handle the garbage collection for the GFS implementation
# by learning which files need to be deleted, informing the chunkservers which
# chunks should be deleted, and informing the database when files no longer
# exist in the system due to deletion.
class Scrubber:

	def __init__(self, data):
		logging.debug("SCRUBBER: Initilizing Scrubber Instance")
		# Get the data from the database's toDelete list
		self.data = data
		# Get the port number from the config file
		self.port = config.port
		# Set a timeout length
		self.SOCK_TIMEOUT = 3


	# Creates a TCP socket connection to the specified IP
	def connect(self, IP, retryCount):
		try:
			logging.debug("SCRUBBER: Initializing connect()")
			# Create a TCP socket instance
			self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			# Set the timeout of the socket
			self.s.settimeout(self.SOCK_TIMEOUT)
			# Allow the socket to re-use address
			self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			# Connect to the chunkserver over the specified port
			self.s.connect((IP, self.port))
			logging.debug("SCRUBBER: TCP Connection Successfully Established")

		# If we are unable to make the connection, retry three more times. If 
		# the retrys do not work 
		except (socket.error, socket.timeout):
			if retryCount < 4:
				self.connect(IP, retryCount + 1)
				logging.warning("SCRUBBER: Could not connect. Retrying...")
			else:
				logging.error("SCRUBBER: Unable to establish a connection.")


	def cleanLocation(self, location, handle):
		# Connect to the chunk server
		self.connect(location, 0)
		try:
			# Send the chunk server a SANITIZE message with the chunk handle
			# so it knows which chunk it is deleting
			fL.send(self.s, 'SANITIZE|' + str(handle))
			logging.debug('SCRUBBER: Sent SANITIZE message to chunkserver')
			# Wait for a response back from the chunk server to verify that
			# the chunk was removed
			data = fL.recv(self.s)
			logging.debug('SCRUBBER: Received response from chunkserver')
			return data
		except (socket.timeout, socket.error) as e:
			logging.error("SCRUBBER: Connection to chunkserver failed with error: " + str(e) + ". Unable to continue for location: " + str(location))
			# Set data to be empty so the deletion process will not continue to 
			# try and delete a file it received no information about.
			data = ""
			return data



	# Inform the chunk servers of which chunks should be deleted, and upon succesful
	# deletion of all chunks in a file, inform the database that the file can be
	# removed from the database.
	def clean(self):
		logging.debug("SCRUBBER: Commencing Clean")

		# For all of the files in the toDelete list
		for fileName in self.data:


			# Create a TCP connection to the master
			self.connect(config.masterip, 0)
			try:
				# Request all the chunks associated with a given file
				fL.send(self.s, "GETALLCHUNKS|" + fileName)
				# Receive all the chunks associated with a file
				data = fL.recv(self.s)

			except (socket.timeout, socket.error) as e:
				logging.error("SCRUBBER: Connection to master failed with error: " + str(e) + ". Unable to continue for file: " + str(fileName))
				# Set data to be empty so the deletion process will not continue to 
				# try and delete a file it received no information about.
				data = ""

			# Be sure the socket is closed.
			self.s.close()


			# Convert the pipe-separated string of chunk handles into a list
			chunkHandles = data.split("|")
			# Ensure there are no "" elements in the list
			chunkHandles = filter(None, chunkHandles)
			# Create a counter for successful chunk deletions
			successfulChunkDelete = 0


			# For each of those chunk handles
			for handle in chunkHandles:

				# Create a counter for successful deletions from a chunkserver
				successDeleteFromCS = 0
				# Create a TCP connection to the master
				self.connect(config.masterip, 0)
				try:
					# Request all the locations associated with a given chunk
					fL.send(self.s, "GETLOCATIONS|" + handle)
					# Receive all the locations associated with a chunk
					data = fL.recv(self.s)

				except (socket.timeout, socket.error) as e:
					logging.error("SCRUBBER: Connection to master failed with error: " + str(e) + ". Unable to continue for chunk: " + str(handle))
					# Set data to be empty so the deletion process will not continue to 
					# try and delete a file it received no information about.
					data = ""

				# Be sure the socket is closed
				self.s.close()


				# Convert the pipe-separated string of locations into a list
				locations = data.split("|")
				# Ensure there are no "" elements in the list
				locations = filter(None, locations)



				# For each location the chunk is stored on
				for location in locations:

					# Send a SANITIZE message to a specified location
					data = self.cleanLocation(location, handle)


					# If the chunk server responds with a success message, increment the success counter
					if data == "SUCCESS":
						logging.debug("SCRUBBER: Chunk successfully removed from chunkserver")
						successDeleteFromCS += 1
						

					# If the chunk server responds with a failure message, DO SOMETHING ELSE!
					elif data == "FAILED":
						retryAck = self.cleanLocation(location, handle)

						if retryAck == "SUCCESS":
							logging.debug("SCRUBBER: Chunk successfully removed from chunkserver")
							successDeleteFromCS += 1

						elif retryAck == "FAILED":
							logging.error("SCRUBBER: Received failure message on chunk delete. Chunkhandle : " + str(handle))

						else: 
							logging.error("SCRUBBER: Unexpected Receive: " + str(data) + " from chunkserver " + str(location))


					# If the chunk server responds with something other than SUCCESS or FAILED, something went wrong.
					else:
						logging.error("SCRUBBER: Unexpected Receive: " + str(data) + " from chunkserver " + str(location))

				# If the success counter is equal to the amount of all the IPs, then
				# all the IPs successfully deleted that chunk, so increment the 
				# successfulChunkDelete counter
				if len(locations) == successDeleteFromCS:
					successfulChunkDelete += 1
				else:
					# Improve error handling to maybe automatically retry
					logging.error("SCRUBBER: Not all chunk location deletes were successful")

			# If the number of successful chunk deletes is equal to the number of chunks
			# associated with the file, then all the chunks for that file have been deleted,
			# so the file entry can be deleted
			if len(chunkHandles) == successfulChunkDelete:
				# Call the database sanitize function, which removes the key/value pair
				# from the database.
				self.connect(config.masterip, 0)
				fL.send(self.s, "SANITIZE|" + fileName)

				#data = fL.recv(self.s)
				self.s.close()
				logging.debug("SCRUBBER: " + str(fileName) + 'successfully sanitized')
				
			else:
				# Improve error handling to automatically resolve problem
				logging.error("SCRUBBER: Not all chunk deletes were successful")






#########################################################################

#			MAIN					

#########################################################################

if __name__ == "__main__":
	try:
		# Create a TCP socket instance
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		# Set the timeout of the socket
		s.settimeout(3)
		# Allow the socket to re-use address
		s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		# Connect to the chunkserver over the specified port
		s.connect((config.masterip, config.port))
		# Send a message to the master asking for the toDelete list
		fL.send(s, "GETDELETEDATA|*")
		# Recieve a response from the master
		data = fL.recv(s)
		# Get a list of all files to be deleted
		toDelete = data.split("|")
		# Make sure the list has no "" elements in it
		toDelete = filter(None, toDelete)

		print toDelete

		# Create an instance of the Scrubber object, and initiate it.
		scrub = Scrubber(toDelete)
		scrub.clean()

	except (socket.error, socket.timeout) as e:
		logging.error("SCRUBBER: Unable to connect with master. Scrubbing not executed, exited with error: " + str(e))








