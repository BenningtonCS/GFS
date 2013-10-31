#!/usr/bin/env python

#################################################################################
#                                                                               #
#               GFS Distributed File System Master		                #
#_______________________________________________________________________________#
#                                                                               #
# Authors:      Erick Daniszewski                                               #
#		Rohail Altaf							#
#		Klemente Gilbert-Espada                                        	#
#										#
# Date:         21 October 2013                                                 #
# File:         master.py	                                                #
#                                                                               #
# Summary:      Manages chunk metadata in an in-memory database,     #
#		maintains an operations log, and executes API commands					#
#                                                                               #
#################################################################################


import socket, threading, random, os, time, config, sys, logging
import functionLibrary as fL


###############################################################################

#               Verbose (Debug) Handling                                      #

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
        logging.basicConfig(level=logging.INFO, filename='masterLog.txt', format='%(asctime)s %(levelname)s : %(message)s')





#########################################################

#		DATABASE CONSTRUCTOR			#

#########################################################





#we define our class for Chunk Metadata
class Chunk:
        def __init__(self, hand, fileN, sequence):
                #chunks have a filename associated with them
                self.fileName = fileN
                #as well as a unique chunkhandle
                self.handle = hand
                #and a sequence number for putting them together in one file
                self.sequenceNumber = sequence
                #as well as a list of chunkservers the chunk is currently being stored on
                self.location = []
                #as well as a flag for deletion
                self.delete = False

class Database:
		
		
        #create an empty database list
        data = []

        #the initialize code should be run when the master boots up,
        #it uses the opp log to create a database, and then updates
        #the locations of the chunks in that database by querying the
        #chunkservers
        def initialize(self):
        	# Visual confirmation for debugging: confirm init of Database
		logging.debug('Initializing Database instance')
        	logging.debug('opening opp log')
		oppLog = open(OPLOG)


                #We build the initial structure of the database from the oppLog
		for line in oppLog:
                     	#we split on pipe, the opp log should be formatted as:
                        #       Operation|Handle|File Name
                        #on each line

                        #we create a list out of each line in the opp log
			logging.debug("line read: " + str(line))
			lineData = line.split('|')
                        #we only adjust the metadata on a create as of version 1
			if lineData[0] == 'CREATE':
                                #we initialize a new chunk with the data from the opp log
                                #and we determine the sequence number based on the highest
                                #existing sequence number currently in the file
               		 	logging.debug("read : " + str(lineData[0]))
				sequence = self.findHighestSequence(lineData[2]) + 1
				newChunk = Chunk(lineData[1], lineData[2], sequence)
                                #and we append that chunk to our database list
				self.data.append(newChunk)

                #Then we ping each chunkserver to figure out what chunks they have on them.

                #first we open the hostfile
		hostFile = open(ACTIVEHOSTSFILE)
		logging.debug(str(hostFile))
		#and go through it line by line
		for line in hostFile:
			logging.debug("going through hostfile")
		    	#new socket
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		#turn on reuseaddr to preempt address already in use error
			s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		        #connect to a listening chunkserver
			logging.debug(str(line))
			s.connect((line, chunkPort))
			logging.debug("Connected to " + str(line) + " through " + str(chunkPort))
			#send the message that audits the chunkserver
			s.send('CONTENTS?' + EOT)
			logging.debug("sent 'Contents?' to : " + str(line))
		        #recieve their reply, which is formatted as chunkhandle1|chunkhandle2|chunkhandle3|...
		        #to make sure we get all data, even if it exceeds the buffer size, we can
		        #loop over the receive and append to a string to get the whole message
			# Receive data
			data = fL.recv(s)

			logging.debug("Received " + str(data) + " from " + str(line))
		        #make a list of all the chunkhandles on the chunkserver
			chunkData = data.split('|')
		        #compare each chunkhandle in our database to the chunkhandles on the server
			for chunk in self.data:
				if chunk.handle in chunkData:
				 #if they overlap, append the current address to the overlappe
					chunk.location.append(line)
		        	#close our connection, for cleanliness
			s.close()
		# Visual confirmation for debugging: confirm successful init of Database
		logging.debug('Database initialization successful!')


	#update() is run when a new chunk is created and the master is already running
        def update(self, chunkHandle, fileN, sequence, location):
        	# Visual confirmation for debugging: confirm init of update()
		logging.debug('Initializing Update')

                #we create a new chunk with the data passed into the function
                sequence = self.findHighestSequence(fileN) + 1
                newChunk = Chunk(chunkHandle, fileN, sequence)
                #and update it's location data
                newChunk.location = location
                #then add that chunk to the database list
                self.data.append(newChunk)
                # Visual confirmation for debugging: confirm successful update
		logging.debug('Database initialization successful!')

        #locate() is run when the API wants to know the locations of a specific chunk
        #it returns a list of location IP's
        def locate(self, chunkHandle):
        	# Visual confirmation for debugging: confirm init of locate()
		logging.debug('Initializing Locate')
                #we go through all the chunks in the database
                for chunk in self.data:
                        #If the chunkhandle they're looking for exists

                        if int(chunk.handle) == int(chunkHandle):
                                #return that chunk's locations
                                print chunk.location                              
                                return chunk.location
				print "chunk with handle", chunkHandle, "not found"
		# Visual confirmation for debugging: confirm success of locate()
		logging.debug('Locate Successful')

        def findHighestSequence(self, fileName):
        	# Visual confirmation for debugging: confirm init of findHighestSequence()
		logging.debug('Initializing Find Highest Sequence')
        	highestSequence = 0
			#we go through all the chunks in the database, and figure out which chunk
			#with the given filename has the highest sequence number
		for chunk in self.data:
			#print "hey, I'm being called"
			#print chunk.sequenceNumber
			
			if chunk.sequenceNumber > highestSequence and chunk.fileName.strip() == fileName.strip():
				#we set highest sequence to whichever chunk is the highest,
				#and we set targetChunk to the chunk we need
				#print chunk.sequenceNumber
				highestSequence = chunk.sequenceNumber
				#print highestSequence
		return highestSequence
		# Visual confirmation for debugging: confirm success of findHighestSequence()
		logging.debug('Highest Sequence Number Found!')

	# returns the file matrix
	def returnData(self):
		# make a list to hold the data
		list = ''
		# for each chunk, add an entry to the list
		for chunk in self.data:
			list += str(chunk.fileName).strip("\n") + "|"
			list += str(chunk.handle)
			list += "@"
		# return that list
		return list



#################################################################

#			API HANDLER OBJECT CREATOR							#

#################################################################



# Define constructor which will build an API-handling thread
class handleCommand(threading.Thread):
	# Define initialization parameters
	def __init__(self, ip, port, socket, data):
		threading.Thread.__init__(self)
		self.ip = ip
		self.port = port
		self.s = socket
		self.data = data
		# Visual confirmation for debugging: see what data was received
		logging.debug('DATA ==> ' + data)
		# Visual confirmation for debugging:confirm that init was successful 
		#and new thread was made
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

	# Function to keep track of chunkHandle numbers and to increment the number
	def handleCounter(self):
		# Visual confirmation for debugging: confirm init of handleCounter()
		logging.debug('Generating chunk handle')
		# Create an empty string to hold the current chunk handle
		chunkHandle = ""
		# Open the text file holding the current chunkHandle number and read it into memory
		with open('handleCounter.txt', 'r') as file:
			self.int = int(file.read())
			chunkHandle = self.int
		# Open the text file holding the current chunkHandle and increment it by 1
		with open('handleCounter.txt', 'w') as file:
			self.int += 1
			file.write(str(self.int))
		# Visual confirmation for debugging: confirm success of handleCounter()
		logging.debug('Successfully generated chunk handle')
		# Return the chunkHandle
		return chunkHandle

	# Function to randomly choose three hosts to hold the copies of the chunk
	def chooseHosts(self):
		# Visual confirmation for debugging: confirm init of chooseHosts()
		logging.debug('Selecting storage locations')

		try:
			# Get a list of all the hosts available
			with open(ACTIVEHOSTSFILE, 'r') as file:
				hostsList = file.read().splitlines()
			# Find how many hosts there are in the list
			lengthList = len(hostsList)
			# Define a low limit
			intLow = 0
			# Randomize between the limits
			randomInt = random.randint(intLow, lengthList)
			# Visual confirmation for debugging: confirm success of chooseHosts()
			logging.debug('Successfully selected storage locations')
			# Return a pipe-seperated list of randomized hosts
			return hostsList[randomInt%lengthList] + "|" + hostsList[(randomInt + 1)%lengthList] + "|" + hostsList[(randomInt + 2)%lengthList]

		except IOError:
			# Handle this error better in the future --> similar to how heartBeat.py
			# needs to handle for this case..
			logging.error( ACTIVEHOSTSFILE + ' does not exist')



	# Function that executes the protocol when a CREATE message is received
	def create(self):
		# Visual confirmation for debugging: confirm init of create()
		logging.debug('Creating chunk metadata')
		# Get a new chunkhandle
		chunkHandle = self.handleCounter()
		# Choose which chunkserver it will be stored on
		hosts = self.chooseHosts()
		# Split the list of locations by pipe
		createLocations = hosts.split('|')
		# Update the database to now include the newly created chunk
		################################################################
		#Should probably later be intergrated better with oplog updates#
		################################################################
		sequence = database.findHighestSequence(self.fileName) + 1
		database.update(chunkHandle, self.fileName, sequence, createLocations)
		# Visual confirmation for debugging: confirm success of create()
		logging.debug('Chunk metadata successfully created')
		try:
			# Send the API a string containing the location and chunkHandle information
			self.s.send(str(hosts) + "|" + str(chunkHandle) + EOT)
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
		targetSequence = database.findHighestSequence(self.fileName)
		# Visual confirmation for debugging: confirm the target sequence
		logging.debug('TARGET SEQUENCE ==> ' + str(targetSequence))

		targetChunk = Chunk(00, 'test', -2)
		for chunk in database.data:
			#print chunk.fileName
			#print fileName
			#print chunk.sequenceNumber
			#print targetSequence
			if chunk.fileName.strip() == self.fileName.strip() and chunk.sequenceNumber == targetSequence:
				targetChunk = chunk
		#then we find the locations of the targetChunk

		###########################################################################
		#IF TARGETCHUNK IS NOT FOUND, A MAJOR ERROR WILL BE THROWN. NEEDS HANDLING#
		###########################################################################
		print "targetChunk handle ==", targetChunk.handle
		locations = database.locate(targetChunk.handle)
		print locations
		#We create an empty message to append our locations to
		appendMessage = ''
		#we prepare to send the locations and chunkhandle to the API
		for item in locations:
			print "adding", item, "to append message"
			#add the location plus a pipe
			appendMessage += item.strip() + '|'
		#append the chunkhandle to our message
		appendMessage += str(targetChunk.handle)
		#send our message
		self.s.send(appendMessage + EOT)
		# Visual confirmation for debugging: confirm send of a list of storage hosts and chunk handle
		logging.debug('SENT == ' + str(appendMessage))
		# Visual confirmation for debugging: confirm success of append()
		logging.debug('Append successfully handled')




	# Function that executes the protocol when a READ message is received
	def read(self):
		byteOffset = int(self.msg[2])
		bytesToRead = int(self.msg[3])
		logging.debug('parsed byte offset and bytes to read')
		# With the byte offSet, we want to modulo it with the chunkSize in the config
		# so we will know what chunk sequence the start of the read will be 

		# Get the size of a chunk from the config file
		maxChunkSize = config.chunkSize
		# Find the sequence of the chunk in the given file by using integer division
		# (divide and take the floor)
		startSequence = byteOffset//maxChunkSize

		# Get the offset of the read-start within its given chunk
		chunkByteOffset = byteOffset%maxChunkSize

		logging.debug('start sequence # == ' + str(startSequence))
		logging.debug('chunk byte offset == ' + str(chunkByteOffset))

		# With the bytesToRead, we want to add it to the byte offset, then modulo that 
		# number to see if it is in the same chunk seqence. If not, then we will have to
		# return multiple locations and chunkHandles

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
		# and the byte offset from within that chunk to begin reading from.
		logging.debug('beginning for loop in read function. Looking for file:' + self.fileName + 'between sequence number ' + str(startSequence + 1) + ' and ' + str(endSequence + 1))
		for sequence in range(startSequence, (endSequence + 1)):
			try:
				seq = sequence + 1 # I know I know I'll fix it soon
				for chunk in database.data:
					logging.debug('checking if statement in for loop: chunk = ' + str(chunk.handle) + ', file name =' + chunk.fileName + 'and sequence = ' + str(seq))
					# If the chunk fileName and sequence number match up, we have the chunk we're looking for
					logging.debug('chunk.sequenceNumber == ' + str(chunk.sequenceNumber))
					logging.debug('seq == ' + str(seq))
					if chunk.fileName.strip() == self.fileName.strip() and chunk.sequenceNumber == seq:
						logging.debug('if statement let us in')
						targetChunk = chunk
						logging.debug('a target chunk has been found')
						# Append a location where the chunk is stored (0th element in the locations list)
						responseMessage += "|" + str(targetChunk.location[0].strip())
						logging.debug('location == ' + str(targetChunk.location))
						# Append the chunk handle
						responseMessage += "*" + str(targetChunk.handle)
						logging.debug('chunk handle == ' + str(targetChunk.handle))
						# Append the byte offset to start reading from
						responseMessage += "*" + str(chunkByteOffset)
						logging.debug('byte offset == ' + str(chunkByteOffset))

						# Check to see if the READ will take place in the same chunk. If it does, append the 
						# endOffset to the message so the client will know where to end reading
						if startSequence == endSequence:
							responseMessage += "*" + str(endOffset)
						# If the READ takes place over multiple chunks, write the end of read for the current
						# chunk to be the end of the chunk, and then increase the sequence number so when the 
						# metadata for the last chunk is processed, it will be caught by the if statement above
						# and send the appropriate ending offset.
						elif startSequence < endSequence:
							responseMessage += "*" + maxChunkSize
							startSequence += 1

						# If the read request spans over more than one chunk, we will start reading
						# the second chunk from where the first chunk left off, that is to say, at the 
						# beginning of the second chunk (and this would be true if for whatever reason
						# we read through the end of the second chunk into a third chunk), so we much now
						# change the byteOffset to be zero so we start reading additional chunks in the 
						# correct place.
						chunkByteOffset = 0
						logging.debug('reset chunk byte offset')
					else:
						logging.error('Chunk ' + str(chunk.handle) + ' not found in database')

			except:
				# If the specific file can not be found in the database, let it be known!
				# Should also send an error message to client so their protocol terminates.
				logging.error("Specified file "  +str(self.fileName) + " does not exist in database")


		logging.debug('RESPONSE MESSAGE == ' + str(responseMessage))
		#send our message
		self.s.send(responseMessage + EOT)
		logging.debug('SENT == ' + responseMessage)
		# Visual confirmation for debugging: confirm success of read()
		logging.debug('Read successfully handled')



	# Function that executes the protocol when a DELETE message is received
	def delete(self):
		logging.debug('Begin updating delete flag to True')
		
		try:
			# Look through all the chunks in the database which have the specified file name
			for chunk in database.data:
				if chunk.fileName.strip() == self.fileName.strip():
					chunk.delete = True
					logging.debug('Delete flag marked True for ' + str(chunk.fileName) + ', chunk : ' + str(chunk.handle))
				else:
					logging.debug('Delete flag unchanged for ' + str(chunk.fileName) + ', chunk : ' + str(chunk.handle))
	
			logging.debug('Delete Flags Updated')

		# Update this exception handling to the case where database is not found
		except:
			logging.error('Fatal Error')	



	def undelete(self):
		logging.debug('Begin updating delete flag to False')

		try:
			# Look through all the chunks in the database which have the specified file name
			for chunk in database.data:
				if chunk.fileName.strip() == self.fileName.strip():
					chunk.delete = False
					logging.debug('Delete flag marked False for ' + str(chunk.fileName) + ', chunk : ' + str(chunk.handle))
				else:
					logging.debug('Delete flag unchanged for ' + str(chunk.fileName) + ', chunk : ' + str(chunk.handle))

			logging.debug('Delete flag updated')

		# Update this exception handling to the case where database is not found
		except:
			logging.error('Fatal error')



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
		self.s.send(list + EOT)

	# Function to handle the message received from the API
	def run(self):
		# Parse the input into the msg variable
		self.msg = self.handleInput(self.data)
		# Define the first item in the list to be the operation name
		self.op = self.msg[0]
		# Define the second item in the list to be the file name
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

#                       OPLOG OBJECT CREATOR                    #

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

#                       MAIN 						                  #

#######################################################################

# Define the paths of the host file, activehost file, and oplog from the config file, and
# define the port to be used, also from the config file
HOSTSFILE = config.hostsfile
ACTIVEHOSTSFILE = config.activehostsfile
OPLOG = config.oplog
chunkPort = config.port
EOT = config.eot

# Make sure the database initializes before anything else is done
database = Database()
database.initialize()


# Create a server TCP socket and allow address re-use
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

# Create a list in which threads will be stored in order to be joined later
threads = []


# Bind the listener-server 
s.bind(('', chunkPort))

logging.info("Master successfully initialized!")
# Main listening thread for API commands
while 1:
	try:
		# Listen for API connections, maintaining a queue of 5 connections
		s.listen(5)
		print "Listening..."
		# Accept the incoming connection
		conn, addr = s.accept()

		# Receive the data
		data = fL.recv(conn)
		
		# When the connection is established and data is successfully acquired,
		# start a new thread to handle the command. Having this threaded allows for
		# multiple commands (or multiple API) to interact with the master at one time
		newThread = handleCommand(addr[0], addr[1], conn, data)
		newThread.start()
		# Append the thread to the thread[] list, which will be joined upon exiting the 
		# while loop
		threads.append(newThread)
	# If someone ends the master through keyboard interrupt, break out of the loop
	# to allow the threads to finsh before closing the main thread
	except KeyboardInterrupt:
		print "Exiting Now."
		break

# When server ends gracefully, wait until remaining threads finish before ending
for t in threads:
	t.join()




