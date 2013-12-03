#!/usr/bin/env python

#################################################################################
#                                                                             
#               GFS Distributed File System Master		              
#________________________________________________________________________________
#                                                                          
# Authors:      Erick Daniszewski                                            
#		Rohail Altaf							
#		Klemente Gilbert-Espada                            
#										
# Date:         21 October 2013                                        
# File:         master.py	                                           
#                                                                      
# Summary:      Manages chunk metadata in an in-memory database,     
#		maintains an operations log, and executes API commands					
#                                                                               
#################################################################################


import socket, threading, random, os, time, config, sys, logging, Queue
import heartBeat as hB
import functionLibrary as fL
import database as db
#import debugging
fL.debug()

#################################################################

#			API HANDLER OBJECT CREATOR		

#################################################################



# Define constructor which will build an API-handling thread
class handleCommand(threading.Thread):
	# Define initialization parameters
	def __init__(self, ip, port, socket, data, lock):
		threading.Thread.__init__(self)
		self.ip = ip
		self.port = port
		self.s = socket
		self.data = data
		self.lock = lock
		# Visual confirmation for debugging: see what data was received
		logging.debug('DATA ==> ' + data)
		# Visual confirmation for debugging:confirm that init was successful 
		# and new thread was made
		logging.debug("Started new thread for " + str(ip) + " on port " + str(port))

	# Funtion to parse input into usable data by splitting at a pipe character
	def handleInput(self, apiInput):
		# Visual confirmation for debugging: confirm init of handleInput()
		logging.debug('Parsing input')
		# Create a list by splitting at a pipe
		input = apiInput.split("|")
		# Visual confirmation for debugging: confirm success of handleInput()
		logging.debug('Successfully parsed input')
		# Return the list
		return input


	# Function that will create a new file in the database
	def create(self):
		# Visual confirmation for debugging: confirm init of create()
		logging.debug('Creating chunk metadata')
		# Acquire a lock so the chunk handle counter can not be accessed simultaneously
		self.lock.acquire()
		# Get a new chunkhandle
		chunkHandle = database.getChunkHandle()
		# Release the lock so others can access the chunk handle counter
		self.lock.release()

		# Create a new file, and store the return flag
		createFileFlag = database.createNewFile(self.fileName, chunkHandle)
		

		# If the return flag was an error flag, alert the logger and API of the error
		if createFileFlag == -1:
			logging.error("Got a duplicate file name, sending FAIL to API")
			fL.send(self.s, "FAIL1")
			return -1

		elif createFileFlag == -2:
			logging.error("No file exists for a chunk to be created for")
			fL.send(self.s, "FAIL2")

		elif createFileFlag== -3:
			logging.error("Chunk is not the latest chunk. New chunk has been created that can be appended to.")
			fL.send(self.s, "FAIL3")


		# Get the locations for a specified chunk
		locations = database.data[self.fileName].chunks[chunkHandle].locations

		# Parse the locations list retreived above into a pipe-separated list
		hosts = ""
		for item in locations:
			hosts += item + "|"

		# Visual confirmation for debugging: confirm success of create()
		logging.debug('Chunk metadata successfully created')

		try:
			# Send the API a string containing the location and chunkHandle information
			fL.send(self.s, str(hosts) + str(chunkHandle))

		except socket.error:
			logging.warning('Socket Connection Broken')
		# Visual confirmation for debugging: confirm send of a list of storage hosts and chunk handle
		logging.debug('SENT ==> ' + str(hosts) + str(chunkHandle))

		# Receieve an ack to affirm that the chunk was successfully created
		ack = fL.recv(self.s)

		if ack == "CREATED":
			logging.debug("successful creation")
		elif ack == "FAILED":
			logging.error("unsuccessful creation")
		else:
			logging.error("unknown ack")
		


	def createChunk(self):
		self.lock.acquire()
		chunkHandle = database.getChunkHandle()
		self.lock.release()
		newChunk = database.createNewChunk(self.msg[1], self.msg[2], chunkHandle)
		fL.send(self.s, newChunk)



	# Function that executes the protocol when an APPEND message is received
	def append(self):
		# Visual confirmation for debugging: confirm init of append()
		logging.debug('Gathering metadata for chunk append')
		
		# We know that we will only be appending to the lastest chunk, since a new
		# chunk should only be created when an old chunk fills up, so we find the 
		# handle of the latest chunk for a given file.
		latestChunkHandle = database.findLatestChunk(self.fileName)
		# Then we get the locations where that chunk is stored
		locations = database.getChunkLocations(latestChunkHandle)

		# Define an empty string that will hold the message we send back to the client
		appendMessage = ''

		# Parse the locations list into a pipe separated string
		for item in locations:
			appendMessage += item + '|'

		# Add the chunk handle to the message we will send to the client
		appendMessage += str(latestChunkHandle)

		#send our message
		fL.send(self.s, appendMessage)
		# Visual confirmation for debugging: confirm send of a list of storage hosts and chunk handle
		logging.debug('SENT == ' + str(appendMessage))
		# Visual confirmation for debugging: confirm success of append()
		logging.debug('Append successfully handled')






	# Function that executes the protocol when a READ message is received
	def read(self):
		# Get the byte offset and bytes to read from the received message
		try:
			byteOffset = int(self.msg[2])
			bytesToRead = int(self.msg[3])
		# If there is an index error (the values do not exist in the message)
		# alert the client and end the read function.
		except IndexError:
			fL.send(self.s, "READ command not given proper parameters")
			return

		logging.debug('parsed byte offset and bytes to read')

		# Get the size of a chunk from the config file
		maxChunkSize = config.chunkSize
		# Find the sequence of the chunk in the given file by using integer division
		# (divide and take the floor)
		startSequence = byteOffset//maxChunkSize

		if startSequence > len(database.data[self.fileName].chunks.keys()):
			logging.debug('No such byte offset exists for the given file')
			fL.send(self.s, "FAILED, NO SUCH BYTE OFFSET EXISTS FOR THIS FILE")
			return

		# Get the offset of the read-start within its given chunk
		chunkByteOffset = byteOffset%maxChunkSize

		logging.debug('start sequence # == ' + str(startSequence))
		logging.debug('chunk byte offset == ' + str(chunkByteOffset))


		# If the user inputs a bytes to read of -1, the read will go until the end
		# of the file.
		if bytesToRead == -1:
			# The ending offset will be the max file size
			endOffset = maxChunkSize
			# The end sequence can be found be knowing how many chunks a file has, and 
			# subtracting by 1 because the sequence numbers start at 0, not 1.
			endSequence = len(database.data[self.fileName].chunks.keys()) - 1

		else:
			# To find where the user wants to read ending, add the number of bytes they want
			# to read to their starting point, the byteOffest. This will give the byte offset
			# of the end of the read
			endReadOffset = byteOffset + bytesToRead

			# Find the sequence of the chunk in which the end of the read will terminate
			endSequence = endReadOffset//maxChunkSize

			# Get the offset of the read-end within its given chunk
			endOffset = endReadOffset%maxChunkSize

		logging.debug('end sequence # == ' + str(endSequence))
		logging.debug('end read offset == ' + str(endOffset))

		# Create an empty string to hold the message that will be sent to the client
		responseMessage = "READFROM"
		

		# For each sequence number that exists between (and including) the read-start chunk
		# and the read-end chunk, get the file's chunk with the appropriate sequence number,
		# and append to the response message, a location it is stored at, its chunk handle, 
		# the byte offset from within that chunk to begin reading from, and the byte offset
		# to end reading from
		for sequence in range(startSequence, (endSequence + 1)):
		#	try:
			# Get the chunkHandles associated with the given file, and sort the chunkHandles from
			# least to greatest in the list. This will put them in their sequence order where the 
			# 0th element is now the 0th sequence, 1st element the 1st sequence, etc.
			logging.debug(sorted(database.data[self.fileName].chunks.keys()))
			associatedChunkHandles = sorted(database.data[self.fileName].chunks.keys())

			# Append a location of where the start-sequence chunk is stored to the message
			logging.debug(database.data[self.fileName].chunks[associatedChunkHandles[sequence]].locations)
			responseMessage += "|" + str(database.data[self.fileName].chunks[associatedChunkHandles[sequence]].locations[0])

			# Append the chunk handle to the message
			responseMessage += "*" + str(associatedChunkHandles[sequence])

			# Append the byte offset to start reading from to the message
			responseMessage += "*" + str(chunkByteOffset)

			# If there are multiple chunks that will be read over, the next chunk will start
			# the read from the beginning
			chunkByteOffset = 0

			# Check to see if the READ will take place in the same chunk. If it does, append the 
			# endOffset to the message so the client will know where to end reading
			if startSequence == endSequence:
				responseMessage += "*" + str(endOffset)
			# If the READ takes place over multiple chunks, write the end of read for the current
			# chunk to be the end of the chunk, and then increase the start sequence number so when the 
			# metadata for the last chunk is processed, it will be caught by the if statement above
			# and send the appropriate ending offset.
			elif startSequence < endSequence:
				responseMessage += "*" + str(maxChunkSize)
				startSequence += 1

		#	except:
		#		logging.error("Unable to generate proper READ response message.")


		logging.debug('RESPONSE MESSAGE == ' + str(responseMessage))
		#send our message
		fL.send(self.s, responseMessage)
		logging.debug('SENT == ' + responseMessage)
		# Visual confirmation for debugging: confirm success of read()
		logging.debug('Read successfully handled')



	# Function that will prompt the database to update the delete flag for 
	# the specified file
	def delete(self):
		logging.debug('Begin updating delete flag to True')

		# Make sure the file is not already marked for delete
		if self.fileName not in database.toDelete:
			# Make sure the file you wish to delete is actually a file in the system
			if self.fileName in database.data.keys():
				try:
					# Change the delete flag for the specified file
					database.flagDelete(self.fileName)
					# Confirm that file has been marked for deletion
					fL.send(self.s, "MARKED")

					logging.debug('Delete Flags Updated')

				except:
					logging.error("File could not be marked for deletion")
					fL.send(self.s, "FAILED1")

			else:
				logging.debug('The file, ' + self.fileName + ', does not exist.')
				fL.send(self.s, "FAILED2")

		else:
			logging.debug('The file, ' + self.fileName + ', is already marked for delete')
			fL.send(self.s, "FAILED3")
		


	# Function that will prompt the database to update the delete flag for 
	# the specified file
	def undelete(self):
		logging.debug('Begin updating delete flag to False')

		# Make sure the file is already marked for delete
		if self.fileName in database.toDelete:
			
			try:
				# Change the delete flag for the specified file
				database.flagUndelete(self.fileName)
				# Confirm that file has been marked for deletion
				fL.send(self.s, "MARKED")

				logging.debug('Delete Flags Updated')

			except:
				logging.error("File could not be unmarked for deletion")
				fL.send(self.s, "FAILED1")

		# If the file is not already marked for delete, you can't undelete it..
		else:
			logging.debug('The file, ' + self.fileName + ', was not marked for deletion to begin with.')
			fL.send(self.s, "FAILED2")


	# When a sanitize request is received, alert the database to delete the 
	# given file name
	def sanitize(self):
		database.sanitizeFile(self.fileName)


	
	# Function that will send the files that should be deleted to the scrubber
	def getDeleteData(self):
		# Get the list of files from the database
		toDelete = database.toDelete
		# Create an empty message which will hold the data
		msg = ""
		# For each item in the list, add it to the message string
		for item in toDelete:
			msg += item + "|"
		# Send back the string of delete data
		fL.send(self.s, msg)


	# Function that will send all of the chunks associated with the given file
	def getAllChunks(self):
		chunks = database.allChunks(self.fileName)
		fL.send(self.s, chunks)

	# Function that will send all the locations of a given chunk
	def getAllLocations(self):
		locations = database.getChunkLocations(self.msg[1])
		msg = ""
		for item in locations:
			msg += item + "|"

		fL.send(self.s, msg)


	# Function that executes the protocol when FILELIST message is received	
	def fileList(self):
		# call the database object's returnData method
		list = str(database.getFiles())
		fL.send(self.s, list)



	# Function to handle the message received from the API
	def run(self):
		# Parse the input into the msg variable
		self.msg = self.handleInput(self.data)
		# The zeroth item in the list of received data should always be the operation
		self.op = self.msg[0]
		
		try:
			# The first item in the list of received data should always be the file name
			self.fileName = self.msg[1]
		except IndexError:
			logging.error("master recieved no file name")
			pass

		# Visual confirmation for debugging: confirm connection
		logging.debug('Connection from: ' + str(self.ip) + " on port " + str(self.port))
		# Visual confirmation for debugging: confirm received operation
		logging.debug('Received operation: ' + str(self.op))


		# Initiate the protocol to create a file/initial chunk
		if self.op == "CREATE":
			self.create()

		# Initiate the protocol to mark a specified file for deletion
		elif self.op == "DELETE":
			self.delete()

		# Initiate the protocol to unmark a specififed file for deletion
		elif self.op == "UNDELETE":
			self.undelete()

		# Initiate the protocol to get metadata to allow for a data append
		elif self.op == "APPEND":
			self.append()

		# Initiate the protocol to retrieve the metadata for a specified file read
		elif self.op == "READ":
			self.read()

		# If the operation is to SANITIZE, initiate cleansing the database:
		elif self.op == "SANITIZE":
			self.sanitize()

		# If the operation is to get the list of all the things to be deleted, do so!
		elif self.op == "GETDELETEDATA":
			self.getDeleteData()

		# If the operation is to get the list of all the chunks associated with a file, do so!
		elif self.op == "GETALLCHUNKS":
			self.getAllChunks()

		# If the operation is to get the list of all the locations of a chunk, do so!
		elif self.op == "GETLOCATIONS":
			self.getAllLocations()

		# Initiate the protocol to get a list of all the files currently in the database
		elif self.op == "FILELIST":
			self.fileList()
						
		# Initiate the protocol to create a new chunk (not a new file). Called on a multichunk append
		elif self.op == "CREATECHUNK":
			self.createChunk()		

		else:
			# If the operation is something else, something went terribly wrong. 
			# This error handling needs to vastly improve
			logging.error("Command " + self.op + " not recognized. No actions taken.")






#######################################################################

#                       DEFINE WORKER FUNCTION FOR QUEUE THREAD

#######################################################################

# The worker function will become threaded and act whenever an item is
# added to the queue
def worker():
	while True:
		# Get an item from the queue
		item = q.get()
		# Define an object that will handle the data passed in by the queue
		handler = handleCommand(addr[0], addr[1], conn, data, threadLock)
		# Run the handler to process the data
		handler.run()
		# Mark the task as complete
		q.task_done()




#######################################################################

#                       THREADED HOSTS CHECKER

#######################################################################

# The hostListener will check to see if the activehosts file has changed 
# since the previous time it was run. If the activehosts file has changed, 
# that indicates that a chunkserver has either left or joined the cluster.
# The hostListener will then take the appropriate steps depending on which
# is the case.
def hostListener():
	logging.debug("Start hostListener")

	# Define a list that will hold the IPs of active chunkservers
	previous = []

	while True:
		print database.locDict
		# Run the heartBeat to get an updated list of active chunkservers
		heartBeat.pumpBlood()

		logging.debug("Reading from activehosts.txt...")
		# Read the active servers out of the activehosts file.
		with open(ACTIVEHOSTSFILE, 'r') as hosts:
			active = hosts.read().splitlines()

		logging.debug("Check for chunkserver arrivals")

		toAppend = []
		# To see if anything has been added, check to see whether something
		# exists now that did not exist previously.
		for item in active:
			if item not in previous:
				logging.debug(str(item) + " joined!")
				# Interrogate the chunk server to find out what it has on it, 
				# and update the databased based on its contents.
				database.interrogateChunkServer(item, 0)
				# Add the now active item to the toAppend list, so it accurate
				# the next time the loop runs.
				logging.debug("Appending to 'previous' list")
				toAppend.append(item)

		# Append all the IPs from the list of new IPs to the previous list
		# so it is accurate the next time the loop runs.
		for item in toAppend:
			previous.append(item)

		logging.debug("Check for chunkserver departures")

		toRemove = []
		# To see if anything left, check to see whether something does not
		# exist now that existed previously.
		for item in previous:
			if item not in active:
				logging.debug(str(item) + " departed!")
				# Call the function that will handle the database update
				# and replication, if needed.
				database.chunkserverDeparture(item)
				# Add the item to the toRemove list, so it is accurate
				# the next time the loop runs.
				logging.debug("Removing from 'previous' list")
				toRemove.append(item)

		# Remove all the IPs from the list of departed IPs to the previous list
		# so it is accurate the next time the loop runs
		for item in toRemove:
			previous.remove(item)


		# As this does not need to be run continuously, we can define
		# how often we wish to run it.
		time.sleep(30)



#######################################################################

#                       MAIN 			

#######################################################################

# Initiate an instance of the database
database = db.Database()
# Initiate an instance of the heartBeat
heartBeat = hB.heartBeat()

if __name__ == "__main__":

	# Define the paths of the host file, activehost file from the config file, and
	# define the port to be used, also from the config file
	HOSTSFILE = config.hostsfile
	ACTIVEHOSTSFILE = config.activehostsfile
	chunkPort = config.port
	EOT = config.eot
	# Define a thread lock to be used to get and increment chunk handles
	threadLock = threading.Lock()

	# Run the heartBeat to make sure there is an up-to-date activehosts.txt file
	heartBeat.pumpBlood()

	# Make sure the database initializes before anything else is done
	database.initialize()

	# Define a queue
	q = Queue.Queue(maxsize=0)

	# Define the number of worker threads to be activated
	WORKERS = 5

	# Create a flag that will allow a single host-listener
	# thread to be started, instead of multiple.
	listenerFlag = 0

	# Initiate the worker threads as daemon threads
	for i in range(WORKERS):
		if listenerFlag == 0:
			listenerFlag = 1
			r = threading.Thread(target=hostListener)
			r.daemon = True
			r.start()

		t = threading.Thread(target=worker)
		t.daemon = True
		t.start()


	# Create a server TCP socket and allow address re-use
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)


	# Bind the listener-server
	s.bind(('', chunkPort))

	logging.info("Master successfully initialized!")
	# Main listening thread for API commands
	while 1:
		try:
			# Listen for API connections
			s.listen(1)
			print "Listening..."
			# Accept the incoming connection
			conn, addr = s.accept()

			# Receive the data
			data = fL.recv(conn)
			# Put the data into a queue so the queue worker can hand the data off to 
			# an instance of the handleCommand object.
			q.put(data)

		# If someone ends the master through keyboard interrupt, break out of the loop
		# to allow the threads to finsh before closing the main thread
		except KeyboardInterrupt:
			print "Exiting Now."
			logging.info("Master stopped by keyboard interrupt")
			break


