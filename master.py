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


import socket, threading, random, os, time, config



# Define the paths of the host file, activehost file, and oplog
HOSTSFILE = config.hostsfile
ACTIVEHOSTSFILE = config.activehostsfile
OPLOG = config.oplog





#########################################################

#		DATABASE CONSTRUCTOR			#

#########################################################


#we have a preset chunkPort for auditing the chunkServers
chunkPort = config.port


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

class Database:

        #create an empty database list
        data = []

        #the initialize code should be run when the master boots up,
        #it uses the opp log to create a database, and then updates
        #the locations of the chunks in that database by querying the
        #chunkservers
        def initialize(self):
        	print "opening opp log"
		oppLog = open(OPLOG)


                #We build the initial structure of the database from the oppLog
		for line in oppLog:
                     	#we split on pipe, the opp log should be formatted as:
                        #       Operation|Handle|File Name
                        #on each line

                        #we create a list out of each line in the opp log
			print "line read:", line
			lineData = line.split('|')
                        #we only adjust the metadata on a create as of version 1
			if lineData[0] == 'CREATE':
                                #we initialize a new chunk with the data from the opp log
                                #and we determine the sequence number based on the highest
                                #existing sequence number currently in the file
               		 	print "read", lineData[0]
				sequence = self.findHighestSequence(lineData[2]) + 1
				newChunk = Chunk(lineData[1], lineData[2], sequence)
                                #and we append that chunk to our database list
				self.data.append(newChunk)

                #Then we ping each chunkserver to figure out what chunks they have on them.

                #first we open the hostfile
		hostFile = open(ACTIVEHOSTSFILE)
		print hostFile
		#and go through it line by line
		for line in hostFile:
			print "going through hostfile"
		    	#new socket
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		#turn on reuseaddr to preempt address already in use error
			s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		        #connect to a listening chunkserver
			print line
			s.connect((line, chunkPort))
			print "connected to", line, "through", chunkPort
			#send the message that audits the chunkserver
			s.send('Contents?')
			print "sent 'Contents?' to", line
		        #recieve their reply, which is formatted as chunkhandle1|chunkhandle2|chunkhandle3|...
		        #to make sure we get all data, even if it exceeds the buffer size, we can
		        #loop over the receive and append to a string to get the whole message
			while 1:
				#receive the data
				d = s.recv(1024)
				#if there is no more data, exit the loop
				if not d:
					break
				#append the received data into a string
				data += d
				
			print "received", data, "from", line
		        #make a list of all the chunkhandles on the chunkserver
			chunkData = data.split('|')
		        #compare each chunkhandle in our database to the chunkhandles on the server
			for chunk in self.data:
				if chunk.handle in chunkData:
				 #if they overlap, append the current address to the overlappe
					chunk.location.append(line)
		        	#close our connection, for cleanliness
			s.close()


	#update() is run when a new chunk is created and the master is already running
        def update(self, chunkHandle, fileN, sequence, location):

                #we create a new chunk with the data passed into the function
                sequence = self.findHighestSequence(fileN) + 1
                newChunk = Chunk(chunkHandle, fileN, sequence)
                #and update it's location data
                newChunk.location = location
                #then add that chunk to the database list
                self.data.append(newChunk)

        #locate() is run when the API wants to know the locations of a specific chunk
        #it returns a list of location IP's
        def locate(self, chunkHandle):
                #we go through all the chunks in the database
                for chunk in self.data:
                        #If the chunkhandle they're looking for exists

                        if int(chunk.handle) == int(chunkHandle):
                                #return that chunk's locations
                                print chunk.location                              
                                return chunk.location
				print "chunk with handle", chunkHandle, "not found"

        def findHighestSequence(self, fileName):
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


database = Database()
database.initialize()





#################################################################

#			API HANDLER OBJECT			#

#################################################################




# Create a server TCP socket and allow address re-use
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

# Create a list in which threads will be stored in order to be joined later
threads = []

# Define a Port variable
userPort = config.port

# Bind the listener-server 
s.bind(('', userPort))





# Define constructor which will build an API-handling thread
class handleCommand(threading.Thread):
	# Define initialization parameters
	def __init__(self, ip, port, socket, data):
		threading.Thread.__init__(self)
		self.ip = ip
		self.port = port
		self.s = socket
		self.data = data
		# Visual confirmation for debugging
		print data
		#print self.data
		print "Started new thread for", ip, "on port", port

	# Funtion to parse input into usable data by splitting at a pipe character
	def handleInput(self, apiInput):
		# Create a list by splitting at a pipe
		input = apiInput.split("|")
		# Return the list
		return input

	# Function to keep track of chunkHandle numbers and to increment the number
	def handleCounter(self):
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
		# Return the chunkHandle
		return chunkHandle

	# Function to randomly choose three hosts to hold the copies of the chunk
	def chooseHosts(self):
		# Get a list of all the hosts available
		with open(HOSTSFILE, 'r') as file:
			hostsList = file.read().splitlines()
		# Find how many hosts there are in the list
		lengthList = len(hostsList)
		# Define a low limit
		intLow = 0
		# Define a high limit
		intHigh = lengthList - 3
		# Randomize between the limits
		randomInt = random.randint(intLow, intHigh)
		# Return a pipe-seperated list of randomized hosts
		return hostsList[randomInt] + "|" + hostsList[randomInt + 1] + "|" + hostsList[randomInt + 2]



	# Function that executes the protocol when a CREATE message is received
	def create(self):
		# Get a new chunkhandle
		chunkHandle = self.handleCounter()
		# Choose which chunkserver it will be stored on
		hosts = self.chooseHosts()
		# Send the API a string containing the location and chunkHandle information
		self.s.send(str(hosts) + "|" + str(chunkHandle))
		print "sent", str(hosts) + "|" + str(chunkHandle)
		# Split the list of locations by pipe
		createLocations = hosts.split('|')
		# Update the database to now include the newly created chunk
		################################################################
		#Should probably later be intergrated better with oplog updates#
		################################################################
		sequence = database.findHighestSequence(fileName) + 1
		database.update(chunkHandle, fileName, sequence, createLocations)
		


	# Function that executes the protocol when an APPEND message is received
	def append(self):
		#in the case of an append, we need to locate the last chunk in a file
		#so we set a Highest Sequence counter to keep track of which chunk
		#is the newest
		targetSequence = database.findHighestSequence(fileName)
		print "target sequence ==", targetSequence
		targetChunk = Chunk(00, 'test', -2)
		for chunk in database.data:
			#print chunk.fileName
			#print fileName
			#print chunk.sequenceNumber
			#print targetSequence
			if chunk.fileName.strip() == fileName.strip() and chunk.sequenceNumber == targetSequence:
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
		self.s.send(appendMessage)
		print "sent message", appendMessage



	# Function that executes the protocol when a DELETE message is received
	def delete(self):
		pass
		
		
		
	# Function that executes the protocol when an OPEN message is received
	def open(self):
		pass
		
		
		
	# Function that executes the protocol when a CLOSE message is received
	def close(self):
		pass



	# Function that executes the protocol when an OPLOG message is received
	def oplog(self):
		print "Data Received: OPLOG"
		# Create a new instance of an opLog object
		self.oplog = opLog()
		# Append to the OpLog the <ACTION>|<CHUNKHANDLE>|<FILENAME>
		self.oplog.append(msg[1]+"|"+msg[2]+"|"+msg[3])



	# Function to handle the message received from the API
	def run(self):
		# Parse the input into the msg variable
		msg = self.handleInput(data)
		# Define the first item in the list to be the operation name
		op = msg[0]
		# Define the second item in the list to be the file name
		fileName = msg[1]
		# Visual confirmation for debugging
		print "connection from: ", self.ip, "on port ", self.port
		print "received message", op

		# If the operation is to CREATE:
		if op == "CREATE":
			self.create()

		# If the operation is to DELETE:
		elif op == "DELETE":
			self.delete()

		# If the operation is to APPEND:
		elif op == "APPEND":
			self.append()

		# If the operation is to OPEN:
		elif op == "OPEN":
			self.open()

		# If the operation is to CLOSE:
		elif op == "CLOSE":
			self.close()

		# If the operation is to update the oplog, OPLOG:
		elif op == "OPLOG":
			self.oplog()
						
		else:
			# If the operation is something else, something went terribly wrong. 
			# This error handling needs to vastly improve
			print "Command not recognized. No actions taken."





#################################################################

#                       OPLOG OBJECT CREATOR                    #

#################################################################

# Define an opLog object
class opLog:

	# Open the opLog file. If it can't open, notify and exit.
        def __init__(self):
            	try:
             		self.logFile = open('opLog.txt', 'a')
		except IOError as e:
               		print "Couldn't open file!"
               		exit()

        # Define a function to append to the opLog
        def append(self, data):
        	try:
               		# append the log of the operation to the oplog
                	self.logFile.write(data + "\n")
                	self.logFile.close()
        	except IOError as e:
                	print "Couldn't write to OpLog!"
                	exit()



#########################################################################

#                       MAIN THREAD - API LISTENER                      #

#########################################################################



# Main listening thread for API commands
while 1:
	try:
		# Listen for API connections, maintaining a queue of 5 connections
		s.listen(5)
		print "Listening....................."
		# Accept the incoming connection
		conn, addr = s.accept()
		# Receive all the data from the connection (Command request)
		while 1:
				#receive the data
				d = conn.recv(1024)
				#if there is no more data, exit the loop
				if not d:
					break
				#append the received data into a string
				data += d
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




