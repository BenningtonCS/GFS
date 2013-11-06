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


import socket, threading, random, os, time, config, sys, logging
import functionLibrary as fL
import database as db




###############################################################################

#               Verbose (Debug) Handling               

###############################################################################


# Setup for having a verbose mode for debugging:
# USAGE: When running program, $python master.py , no debug message will show up
# Instead, the program should be run in verbose, $python master.py -v , for debug 
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
        logging.basicConfig(level=logging.INFO, filename='masterLog.log', format='%(asctime)s %(levelname)s : %(message)s')





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

		database.createNewFile(self.fileName, chunkHandle)

		locations = database.data[fileName].chunks[chunkHandle].locations

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
		logging.debug('SENT ==> ' + str(hosts) + "|" + str(chunkHandle))
		


	# Function that executes the protocol when an APPEND message is received
	def append(self):
		# Visual confirmation for debugging: confirm init of append()
		logging.debug('Gathering metadata for chunk append')
		#in the case of an append, we need to locate the last chunk in a file
		#so we set a Highest Sequence counter to keep track of which chunk
		#is the newest

		latestChunkHandle = database.findLatestChunk(self.fileName)
		locations = database.getChunkLocations(self.fileName)

		appendMessage = ''

		for item in locations:
			appendMessage += item + '|'

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

		# Get the offset of the read-start within its given chunk
		chunkByteOffset = byteOffset%maxChunkSize

		logging.debug('start sequence # == ' + str(startSequence))
		logging.debug('chunk byte offset == ' + str(chunkByteOffset))

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
			try:
				# Get the chunkHandles associated with the given file, and sort the chunkHandles from
				# least to greatest in the list. This will put them in their sequence order where the 
				# 0th element is now the 0th sequence, 1st element the 1st sequence, etc.
				associatedChunkHandles = database.data[self.fileName].chunks.keys().sort()

				# Append a location of where the start-sequence chunk is stored to the message
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
					responseMessage += "*" + maxChunkSize
					startSequence += 1

			except:
				logging.error("Unable to generate proper READ response message.")


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

		database.flagDelete(self.fileName)

		logging.debug('Delete Flags Updated')


	# Function that will prompt the database to update the delete flag for 
	# the specified file
	def undelete(self):
		logging.debug('Begin updating delete flag to False')

		database.flagUndelete(self.fileName)

		logging.debug('Delete flag updated')



	# Function that executes the protocol when an OPEN message is received
	def open(self):
		pass
		
		
		
	# Function that executes the protocol when a CLOSE message is received
	def close(self):
		pass



	# Function that executes the protocol when an OPLOG message is received
	def oplog(self):
		# Visual confirmation for debugging: confirm init of oplog()
		logging.debug('Initializing oplog append')
		# Create a new instance of an opLog object
		self.oplog = opLog()
		# Append to the OpLog the <ACTION>|<CHUNKHANDLE>|<FILENAME>
		self.oplog.append(self.msg[1]+"|"+self.msg[2]+"|"+self.msg[3])
		# Visual confirmation for debugging: confirm success of oplog()
		logging.debug('Oplog append successful')
	
	# Function that executes the protocol when FILELIST message is received	
	def fileList(self):
		# call the database object's returnData method
		list = str(database.returnData())
		fL.send(self.s, list)

	# Function to handle the message received from the API
	def run(self):
		# Parse the input into the msg variable
		self.msg = self.handleInput(self.data)
		# The zeroth item in the list of received data should always be the operation
		self.op = self.msg[0]
		# The first item in the list of received data should always be the file name
		self.fileName = self.msg[1]
		# Visual confirmation for debugging: confirm connection
		logging.debug('Connection from: ' + str(self.ip) + " on port " + str(self.port))
		# Visual confirmation for debugging: confirm received operation
		logging.debug('Received operation: ' + str(self.op))


		# If the operation is to CREATE:
		if self.op == "CREATE":
			self.create()

		# If the operation is to DELETE:
		elif self.op == "DELETE":
			self.delete()

		# If the operation is to UNDELETE:
		elif self.op == "UNDELETE":
			self.undelete()

		# If the operation is to APPEND:
		elif self.op == "APPEND":
			self.append()

		# If the operation is to READ:
		elif self.op == "READ":
			self.read()

		# If the operation is to OPEN:
		elif self.op == "OPEN":
			self.open()

		# If the operation is to CLOSE:
		elif self.op == "CLOSE":
			self.close()

		# If the operation is to update the oplog, OPLOG:
		elif self.op == "OPLOG":
			self.oplog()

		elif self.op == "FILELIST":
			self.fileList()
						
		else:
			# If the operation is something else, something went terribly wrong. 
			# This error handling needs to vastly improve
			logging.error("Command " + self.op + " not recognized. No actions taken.")






#################################################################

#                       OPLOG OBJECT CREATOR       

#################################################################

# Define an opLog object
class opLog:

	# Open the opLog file. If it can't open, notify and exit.
        def __init__(self):
            	try:
             		self.logFile = open(OPLOG, 'a')
		except IOError:
               		logging.error('Could not open: ' + OPLOG)
               		exit()

        # Define a function to append to the opLog
        def append(self, data):
        	try:
               		# append the log of the operation to the oplog
                	self.logFile.write(data + "\n")
                	self.logFile.close()
        	except IOError:
                	logging.error('Could not write to: ' + OPLOG)
                	exit()



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

#                       MAIN 			

#######################################################################

# Define the paths of the host file, activehost file, and oplog from the config file, and
# define the port to be used, also from the config file
HOSTSFILE = config.hostsfile
ACTIVEHOSTSFILE = config.activehostsfile
OPLOG = config.oplog
chunkPort = config.port
EOT = config.eot
# Define a thread lock to be used to get and increment chunk handles
threadLock = threading.Lock()

# Define a queue
q = Queue.Queue(maxsize=0)

# Define the number of worker threads to be activated
WORKERS = 5

# Make sure the database initializes before anything else is done
database = db.Database()
database.initialize()


# Initiate the worker threads as daemon threads
for i in range(WORKERS):
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



