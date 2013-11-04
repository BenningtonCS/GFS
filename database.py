#########################################################

#		DATABASE CODE			#

#########################################################

import functionLibrary as fL
import config, logging

#we define our class for Chunk Metadata
class Chunk:
        def __init__(self, hand):
                #chunks have a filename associated with them
                #self.fileName = fileN
                #as well as a unique chunkhandle
                self.handle = hand
                #as well as a list of chunkservers the chunk is currently being stored on
                self.location = []
                #as well as a flag for deletion
                self.delete = False

#files are just objects that store a list of chunks associated with them.
class File:
	def __init__(self, name):
		#they also have individual names
		self.name = name
		self.chunk = []

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
                        
			logging.debug("line read from opp log: " + str(line))

			#we create a list out of each line in the opp log
			lineData = line.split('|')
            
                        #we only adjust the metadata on a create.
                        #once delete is implemented, handling for that should be added here.
			if lineData[0] == 'CREATE':
                                #we initialize a new file with the data from the opp log
               		 	logging.debug("read : " + str(lineData[0]))
               	#the third argument we're passed should become the name of our new file.
				newFile = File(lineData[2])
				newChunk = Chunk(lineData[1])
                                #and we append that chunk to our database list
                self.data.append(newFile)
				self.data[-1].chunk.append(newChunk)

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
			fL.send(s, 'CONTENTS?')
			logging.debug("sent 'Contents?' to : " + str(line))
		        #recieve their reply, which is formatted as chunkhandle1|chunkhandle2|chunkhandle3|...
		        #to make sure we get all data, even if it exceeds the buffer size, we can
		        #loop over the receive and append to a string to get the whole message
			# Define a string that will hold all of the received data
			data = fL.recv(s)

			logging.debug("Received " + str(data) + " from " + str(line))
		        #make a list of all the chunkhandles on the chunkserver
			chunkData = data.split('|')
		        #compare each chunkhandle in our database to the chunkhandles on the server
			for GFSfile in self.data:
				for chunk in GFSfile.chunk
					if chunk in chunkData:
				 #if they overlap, append the current address to the overlap
						chunk.location.append(line)
		        	#close our connection, for cleanliness
			s.close()
		# Visual confirmation for debugging: confirm successful init of Database
		logging.debug('Database initialization successful!')

		def createNewFile(self, fileName):
			self.data.append(File(fileName))
			appendNewChunk(fileName, -1)

		#appendNewChunk is given a file Name and a triggering chunk,
		#it checks to see if a new chunk has already been created, 
		#and if it hasn't it creates one and return's it's chunkhandle.
		#in the event that a new chunk already exists, that chunk's handle
		#is returned instead.
		def appendNewChunk(self, fileName, handleOfFullChunk):
			#we find our file
			for dataFile in self.data:
				#if our file actually exists(it should)
				if dataFile.name == fileName:
					if dataFile.chunk[-1].handle == handleOfFullChunk or handleOfFullChunk == -1:
						#we create a new chunk, with a fresh handle
						newChunk = Chunk(fL.handleCounter())
						#we give it three new chunkservers to live on
						locations = fL.chooseHosts().split

						#append those chunkservers to our chunk's location list
						for location in locations:
							newChunk.location.append(location)
						dataFile.chunk.append(newChunk)
						#give our chunkHandle back to the top 
						return newChunk.handle
					else:
						logging.debug('chunk given to appendNewChunk not most' + 
							' recent chunk in file')
						return -1

			#if we get this far, we never found the fileName.
			logging.debug('failed to find ' + fileName + 
				' in database, appendNewChunk() failed')
			return -1

		def getChunkLocations(self, chunkHandle):
			for dataFile in self.data:
				for chunk in datafile.chunk
					if chunk.handle == chunkHandle:
						return chunk.location
			return -1

		def findLatestChunk(self, fileName):
			for dataFile in self.data
				if dataFile.name == fileName:
					return dataFile.chunk[-1].handle
			return -1

		def appendRecord(self, fileName):

		def read(self, fileName, byteOffset, amountToRead):