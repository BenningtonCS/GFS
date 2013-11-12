#################################################################################
#                                                                               
#               GFS Distributed File System HeartBeat				
#________________________________________________________________________________
#                                                                              
# Authors:      Erick Daniszewski 
#		Klemente Gilbert-Espada                                             
#                                                                              
# Date:         5 November 2013                                                
# File:         database.py                                                   
#                                                                              
# Summary:      database.py creates and maintains the metadata for all the 
#		chunks in the file system. 
#                                        
#################################################################################


import functionLibrary as fL
import config, logging, socket

###############################################################################

#               Verbose (Debug) Handling                        

###############################################################################


# Setup for having a verbose mode for debugging:
# USAGE: When running program, $python database.py , no debug message will show up
# Instead, the program should be run in verbose, $python database.py -v , for debug 
# messages to show up

#logging.basicConfig(level=logging.DEBUG, filename='masterLog.log', format='%(asctime)s DATABASE %(levelname)s : %(message)s')




HOSTSFILE = config.hostsfile
ACTIVEHOSTSFILE = config.activehostsfile
OPLOG = config.oplog
chunkPort = config.port
EOT = config.eot

###############################################################################

#               OBJECT DEFINITIONS                

###############################################################################

# This is the object that is stored in the data dictionary. It's key will be the
# fileName. The object holds its fileName, a delete flag, and a dictionary of 
# the chunks associated with it, where the chunk handle is the key, and the 
# chunk object is the value.
class File:
	def __init__(self, name):
		self.fileName = name
		self.chunks = {}
		self.delete = False

# The chunk object is stored in the chunk library of the File object, keyed to 
# its chunk handle. The chunk object stores the locations where the chunks are 
# being stored.
class Chunk:
	def __init__(self):
		self.locations = []



class Database:
	# Create an empty dictionary to store the chunks, keyed to the file name, eg. {"file1": fileObject}
	data = {}

	# Create an empty dictionary to be used as a chunk --> fileName lookup, eg. {"127":"file3"}
	lookup = {}

	# Create an empty list that will hold fileNames of files flagged for deletion, so
	# the scrubber does not need to waste resources parsing through the data dictionary
	toDelete = []

	# Create a counter for the chunkHandle
	chunkHandle = 0

	
	# Initialization function that will set up the database from the opLog and data
	# from the chunkservers
	def initialize(self):
		logging.debug('Initializing database')
		# Populate the database by parsing the operations log
		self.readFromOpLog()
		# Get/update the locations of all the chunks from the chunkservers
		self.interrogateChunkServers()
		# Now that the database is setup, go through the used chunkhandles and 
		# set the chunk handle counter to the next unused number
		self.updateChunkCounter()


		####### DEBUG MESSAGES TO MAKE SURE THINGS INITIALIZED AS EXPECTED #######
		# The database dictionary
		print self.data
		# The lookup dictionary
		print self.lookup
		# The list of files to delete
		print self.toDelete
		# The current chunk handle
		print self.chunkHandle
		##########################################################################

		logging.debug('Database initialized')



	# This function gets the data from the opLog and parses it to populate the database
	# so it can return to where it left off (convert data in a hard state to soft state, essentially)
	def readFromOpLog(self):
		logging.debug('Initialize readFromOpLog()')
		# Read the contents of the oplog into a list
		with open(OPLOG, 'r') as oplog:
			opLog = oplog.read().splitlines()

		logging.debug('Got contents of opLog')
		# For every entry in the opLog
		for line in opLog:
			# Separate the string by pipe into a list where the list should be formatted:
			# 		[<OPERATION>, <CHUNKHANDLE>, <FILENAME>]
			#		[ 	   0    ,       1      ,      2    ]
			lineData = line.split("|")


			# If the operation was to create a file, create a new file object and 
			# add it to the database dictionary
			if lineData[0] == 'CREATEFILE':
				# Create a new instance of the File object, taking its file name as
				# a parameter
				file = File(lineData[2])
				# Create a new entry in the database, where the file name is the key
				# and the file object is the value
				self.data[lineData[2]] = file
				logging.debug('CREATEFILE ==> new file, ' + str(lineData[2]) + ', added to database')

			# If the operation was to create a chunk, create a new chunk object and 
			# add it to the database
			elif lineData[0] == 'CREATECHUNK':
				# Create a new instance of the Chunk object
				chunk = Chunk()
				# In the file object associated with the file name the chunk belongs to,
				# add the newly created chunk to the chunk dictionary, where the chunk
				# object is the value and the chunk handle is the key
				self.data[lineData[2]].chunks[lineData[1]] = chunk
				# Update the lookup dictionary with the chunk/fileName pair
				self.lookup[lineData[1]] = lineData[2]
				logging.debug('CREATECHUNK ==> new chunk, ' + str(lineData[1]) + ', added to database')

			# If the operation was to delete a file, change the file object's delete attribute
			# to True, so the scrubber will recognize it as marked for deletion.
			elif lineData[0] == 'DELETE':
				self.flagDelete(lineData[2])
				logging.debug('DELETE ==> ' + str(lineData[2]) + ' marked True for delete')

			# If the operation was to undelete a file, change the file object's delete attribute
			# back to False, so the scrubber will not delete it.
			elif lineData[0] == "UNDELETE":
				self.flagUndelete(lineData[2])
				logging.debug('UNDELETE ==> ' + str(lineData[2]) + ' marked False for delete')

			# If the operation was to sanitize, that is, the chunks were actually deleted,
			# as opposed to marked for deletion, then remove the metadata for the file and
			# associated chunks from the database
			elif lineData[0] == "SANITIZED":
				self.sanitizeFile(lineData[2])
				logging.debug('SANITIZED ==> ' + str(lineData[2]) + ' cleansed from chunkservers')

		logging.debug('readFromOpLog() complete')

	# Communicates with all the chunkservers and requests the chunkhandles of all the chunks
	# residing on them. It then appends the locations of a chunk into the appropriate chunk object.
	def interrogateChunkServers(self):
		logging.debug('Initialize interrogateChunkServers()')
		# Read the contents of the activehosts file into a list
		with open(ACTIVEHOSTSFILE, "r") as hosts:
			hostList = hosts.read().splitlines()

		for IP in hostList:
			# COULD USE A TRY/EXCEPT IN HERE PROBABLY IN CASE THE CONNECTION DOES NOT WORK
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			data = " "
			try:
				s.connect((IP, chunkPort))
				logging.debug('Connection Established: ' + str(IP) + ' on port ' + str(chunkPort))
				fL.send(s, 'CONTENTS?')
				logging.debug('Sent chunkserver a CONTENTS? message')
				data = fL.recv(s)
			except:
				logging.error("interrogateChunkServers failed to connect to " + IP)

			
			s.close()
			logging.debug('Received response from chunkserver')

			# If the chunkserver has nothing on it, it should return whitespace. If this is the case, 
			# then nothing in the database can be updated, so it will continue onto the next IP.
			# If the chunkserver returns something other than whitespace (a message formatted chunk1|chunk2|... )
			# then the data can be processed.
			if data != " ":
				# Convert the pipe separated string into a list
				chunkData = data.split('|')

				# For every chunk handle in that list, update that chunk objects locations list
				for chunk in chunkData:
					# ADD SOME ERROR HANDLING HERE -- IF THE CHUNK DOES NOT EXIST IN THE 
					# LOOKUP SOMETHING WENT TERRIBLY WRONG!
					try:
						# Find which file the chunk is associated with in the lookup dictionary
						fileName = self.lookup[chunk]

						# From the file name we found the chunk to be associated with in the
						# lookup, we can append the current IP to the list of chunk locations
						# in the chunk object within the file object dictionary.
						self.data[fileName].chunks[chunk].locations.append(IP)
						logging.debug('Appended location to chunk ' + str(chunk) + ' location list')

					except KeyError:
						logging.warning('Chunk: ' + str(chunk) + ' on ' + str(IP) + ' is not associated with a file in the database. Ignoring chunk and moving on...')

		logging.debug('interrogateChunkServers() complete')



	# Gets a list of all the chunkhandles currently in use, then updates the chunkhandle
	# counter to the next unused number.
	def updateChunkCounter(self):
		# If the lookup is empty (no files/chunks in the file system), set the chunkHandle to 0
		if self.lookup.keys() == []:
			self.chunkHandle = 0
		else:
			# Get the max chunk handle from the database, and add one to get the new chunkhandle
			self.chunkHandle = int(max(self.lookup.keys())) + 1



	# createNewFile adds a new file key and file object to the database, but does not
	# create any chunks associated with that file.
	def createNewFile(self, fileName, cH):
		# Check to see if the fileName already exists
		if fileName in self.data:
			logging.error('file name already exists in database')
			# Return -1 to the master, signifying that the fileName already exists,
			# so the master can alert the client.
			return -1

		else: 
			# Create a new instance of the File object
			file = File(fileName)
			# Add the file object, keyed to the file name, to the database
			self.data[fileName] = file
			# Update the opLog that a new file was created
			fL.appendToOpLog("CREATEFILE|-1|" + fileName)
			self.createNewChunk(fileName, -1, cH)
			logging.debug('createNewFile() ==> new file successfully added to database')

	# createNewChunk is given a file name and a triggering chunk. It checks to see if a 
	# new chunk has already been created, and if it hasn't, it creates one and returns
	# its chunkhandle. In the event that a new chunk altrady exists, that chunk's handle
	# is returned instead.
	def createNewChunk(self, fileName, handleOfFullChunk, cH):
		# Check to see if the specified filename exists in the database
		if fileName not in self.data:
			logging.error('createNewChunk() ==> file for new chunk does not exist')
			# Return an error flag to be parsed by the master, so it can alert the client
			# that the file name does not exist
			return -2

		else:
			# Define the latest chunk to be less than the possible chunkhandles so it will 
			# fail the handleOfFullChunk == latestChunk below. This way, if the message for 
			# handleOfFullChunk that we get is not -1 (the flag for a file created), then 
			# it will not erroneously make a chunk.
			latestChunk = -2

			# If the handleOfFullChunk is not the flag for a file create (where no latest
			# chunk would exist), then find the chunk with the highest chunk handle (the 
			# chunk with the highest sequence number)
			if handleOfFullChunk != -1:
				latestChunk = self.findLatestChunk(fileName)

			# Check to see if the triggering chunk is in the list of chunk handles associated
			# with that file. If it is, that means a new chunk must be created
			if int(handleOfFullChunk) == int(latestChunk) or int(handleOfFullChunk) == -1:
				# Create a new instance of the Chunk object
				chunk = Chunk()
				# Get a new chunkHandle
				chunkHandle = cH
				logging.debug('Got new chunk handle')
				# Add the chunk to the file object, keyed to its new chunkHandle
				self.data[fileName].chunks[chunkHandle] = chunk
				# Get the three locations where the chunk will be stored
				locations = fL.chooseHosts().split("|")
				logging.debug(locations)

				# Append the chunkserver locations to the chunk's location list
				for location in locations:
					logging.debug('adding locations to new chunk')
					self.data[fileName].chunks[chunkHandle].locations.append(location)
					logging.debug('Appending locations to chunk ' + str(chunkHandle) + ' locations list')

				logging.debug('file: ' + fileName + ' chunk: ' + str(self.data[fileName].chunks[chunkHandle]))

				# Add the chunk to the chunk/file lookup
				self.lookup[chunkHandle] = fileName

				print self.data[fileName].chunks[chunkHandle].locations

				# Update the opLog that a new chunk was created
				fL.appendToOpLog("CREATECHUNK|" + chunkHandle + "|" + fileName)

				#If this completed successfully, return a 1.
				return 1

			# The full chunk is not the latest chunk, so a new chunk has already been created for 
			# the file. We dont want to branch a file into multiple chunks, so we let the master know
			# that a new chunk already exists to append to.
			else:
				return -3


	# This function is called to get and increment the current chunk handle from soft state
	def getChunkHandle(self):
		# Increment the chunk handle
		self.chunkHandle += 1
		# Return the current chunkhandle
		return str(self.chunkHandle - 1)


	# This function is used to get the locations where a specified chunk is stored on
	# the chunkservers
	def getChunkLocations(self, chunkHandle):
		logging.debug('Initialize getChunkLocations()')
		# Find the file name associated with the chunk
		logging.debug("chunkHandle is " + chunkHandle)
		fileName = self.lookup[chunkHandle]
		# Return the list of locations belonging to that chunk
		return self.data[fileName].chunks[chunkHandle].locations


	# To find the most recent chunk, instead of maintaining a chunk counter, we rely on the 
	# fact that chunk are created sequentially, meaning that a more recent chunk will have
	# a higher chunk handle. This function looks at all the chunk handles associated with a 
	# specified file and takes returns the highest number chunk handle it found.
	def findLatestChunk(self, fileName):
		logging.debug('Initialize findLatestChunk()')
		# Get a list of all the chunkHandles associated with the file
		associatedChunks = self.data[fileName].chunks.keys()
		# Create an empty list that will contain the chunkHandles as integers
		keyValues = []
		# Convert the chunkHandles into integers
		for item in associatedChunks:
			keyValues.append(int(item))
		logging.debug('Latest Chunk found')
		# Return the highest chunk number as a string
		return str(max(keyValues))


	# When the user wishes to delete a file, it will not be deleted automatically, instead
	# it will be flagged for deletion. This function is called to change the file delete flag
	# from False to True, and put it in the toDelete list, so garbage collection will know
	# which files it must scrub.
	def flagDelete(self, fileName):
		logging.debug('Initialize flagDelete()')
		# Flag the given file for deletion
		self.data[fileName].delete = True
		# Add the file name to the list of files to be deleted
		self.toDelete.append(fileName)
		# Update the opLog that a new file was created
		fL.appendToOpLog("DELETE|-1|" + fileName)

		logging.debug('Delete flag updated')


	# If the user accidentally flagged a file for deletion, there is a period of time between 
	# the flag change and garbage collection in which the delete flag can be set back to False. 
	# This function changes the flag back to false and removes the file from the toDelete list.
	def flagUndelete(self, fileName):
		logging.debug('Initialize flagUndelete()')
		# Unflag the given file for deletion
		self.data[fileName].delete = False
		# Remove the file name from the list of files to be deleted
		self.toDelete.remove(fileName)
		# Update the opLog that a new file was created
		fL.appendToOpLog("UNDELETE|-1|" + fileName)

		logging.debug('Delete flag updated')


	# When the scrubber informs the master that all chunks associated with a file have been deleted
	# from the chunkservers, this function will be called to remove the file from the database.
	def sanitizeFile(self, fileName):
		# Before we delete the file from the database, we want to make sure we know which chunks
		# are associated with it, so they can be removed from the lookup.
		associatedChunks = self.data[fileName].chunks.keys()
		# For each key, remove it from the lookup
		for chunk in associatedChunks:
			del self.lookup[str(chunk)]

		# We also want to make sure that it is no longer in the toDelete list, since it has been deleted
		if fileName in self.toDelete:
			self.toDelete.remove(fileName)

		# Delete the specified key/value pair from the database.
		del self.data[fileName]

		# Update the opLog that a new file was created
		fL.appendToOpLog("SANITIZED|-1|" + fileName)

		logging.debug('sanitizeFile() success')







