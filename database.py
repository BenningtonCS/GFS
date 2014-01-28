#################################################################################
#                                                                               
#               GFS Distributed File System Database				
#________________________________________________________________________________
#  
# Authors:      Erick Daniszewski ; Klemente Gilbert-Espada
# Date:         5 November 2013 
# File:         database.py 
#                                                                              
# Summary:		database.py creates and maintains the metadata for all the 
#				chunks in the file system. 
#                                        
#################################################################################

import functionLibrary as fL
import config, logging, socket, API, random, listener, heartBeat



###############################################################################

#               SETUP                

###############################################################################


# Define the common parameters from the config file
ACTIVEHOSTSFILE = config.activehostsfile
OPLOG = config.oplog
chunkPort = config.port

# Initiallize an instance of the API, to be used for chunk replication
api = API.API()
# Initialize an instance of the heartBeat, to be used for ensuring connection 
# to chunkserver is actually active.
hB = heartBeat.heartBeat()



###############################################################################

#               OBJECT DEFINITIONS                

###############################################################################

# The file object is stored in the data dictionary, keyed to its associated
# fileName. The object holds its fileName, a delete flag, and a dictionary of 
# the chunks associated with it, where the chunk handle is the key, and the 
# chunk object is the value.
class File:
	def __init__(self, name):
		self.fileName = name
		self.chunks = {}
		self.delete = False
		self.Open = False

# The chunk object is stored in the chunk dictionary of the File object, keyed to 
# its chunk handle. The chunk object stores the locations where the chunks are 
# being stored.
class Chunk:
	def __init__(self):
		self.locations = []



# The database object stores all of the metadata for the chunks and contains all the 
# opertions assocaited with database management and manipulation
class Database:

	# Create an empty dictionary to store file objects, keyed to the file name, eg. {"file1": fileObject}
	data = {}
	# Create an empty dictionary to be used as a chunk --> fileName lookup, eg. {"127":"file3"}
	lookup = {}
	# Create an empty list that will hold fileNames of files flagged for deletion, so
	# the scrubber does not need to waste resources parsing through the data dictionary
	toDelete = []
	# Create a counter for the chunkHandle
	chunkHandle = 0
	# Create a dictionary that will be used as a location --> chunk lookup, eg. {"10.10.117.10":[127]}
	locDict = {}
	
	# Initialization function that will set up the in-memory database from parsing the opLog and 
	# from querying the chunkservers
	def initialize(self):
		logging.debug('Initializing database')


		# Get a list of active hosts from an active hosts list
		try:
			with open(ACTIVEHOSTSFILE, 'r') as activeFile:
				activeHosts = activeFile.read().splitlines()

		# If unable to read the file, log the error and alert the listener
		except IOError:
			logging.error(ACTIVEHOSTSFILE + " was unable to be read.")
			listener.logError("DATABASE: Unable to read active hosts on initialize.")
			# Define activeHosts to be empty so initialization can continue.
			activeHosts = []


		# Populate the database by parsing the operations log
		self.readFromOpLog()

		for item in activeHosts:
			# Get/update the locations of all the chunks from the chunkservers
			self.interrogateChunkServer(item, 0)
		# Now that the database is setup, go through the used chunkhandles and 
		# set the chunk handle counter to the next unused number
		self.updateChunkCounter()


		####### DEBUG MESSAGES TO MAKE SURE THINGS INITIALIZED AS EXPECTED #######
		# The database dictionary
		logging.debug(self.data)
		# The lookup dictionary
		logging.debug(self.lookup)
		# The list of files to delete
		logging.debug(self.toDelete)
		# The current chunk handle
		logging.debug(self.chunkHandle)
		# The location lookup dictionary
		logging.debug(self.locDict)
		##########################################################################

		

		# Logs and displays a critical warning that an insufficient number of chunkservers
		# are active, which would lead to poor replication strategies and poor performance
		if len(activeHosts) < 3:
			logging.critical("\nLESS THAN THREE CHUNKSERVERS ARE ACTIVE. OPERATIONS MAY BE LIMITED OR INACCESSIBLE.\n")

		logging.debug('Database initialized')



	# This function gets the data from the opLog and parses it to populate the database
	# so it can return to where it left off (convert data in a hard state to soft state, essentially)
	def readFromOpLog(self):
		logging.debug('Initialize readFromOpLog()')


		try:
			# Read the contents of the oplog into a list
			with open(OPLOG, 'r') as oplog:
				opLog = oplog.read().splitlines()

		# If the database in unable to read the opLog, log it, and alert the listener
		except IOError:
			logging.critical(OPLOG + " was unable to be read.")
			listener.logError("DATABASE: Unable to read oplog on initialize.")
			# If we are unable to read from the opLog, something went terribly wrong
			# and we no longer have a map from files -> chunks. Alert the fatal error
			# and exit, to minimize the risk of writing over existing chunks further 
			# down the line.
			logging.error("Database could not be built reliably. To maintain integrity of existing chunks, exiting database.")
			exit(0)


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
				# Flag the given file for deletion
				self.data[lineData[2]].delete = True
				# Add the file name to the list of files to be deleted
				self.toDelete.append(lineData[2])
				logging.debug('DELETE ==> ' + str(lineData[2]) + ' marked True for delete')

			# If the operation was to undelete a file, change the file object's delete attribute
			# back to False, so the scrubber will not delete it.
			elif lineData[0] == "UNDELETE":
				# Flag the given file for deletion
				self.data[lineData[2]].delete = False
				# Add the file name to the list of files to be deleted
				self.toDelete.remove(lineData[2])
				logging.debug('UNDELETE ==> ' + str(lineData[2]) + ' marked False for delete')

			# If the operation was to sanitize, that is, the chunks were actually deleted,
			# as opposed to marked for deletion, then remove the metadata for the file and
			# associated chunks from the database
			elif lineData[0] == "SANITIZED":
				self.sanitizeFile(lineData[2])
				logging.debug('SANITIZED ==> ' + str(lineData[2]) + ' cleansed from chunkservers')

		logging.debug('readFromOpLog() complete')



	# Function that will allow you to remove an element from the active hosts list.
	def remFromAhosts(self, IP):
		# If the # of attempts exceeds the retry limit, remove the server from the active
		# hosts list.
		with open(self.AHOSTS, "r") as fileActive:
			activeServers = fileActive.read().splitlines()

		# Remove the failing IP from the list of active IPs
		activeServers.remove(IP)

		# Rewrite the active hosts list, now without the failing IP on it.
		with open(self.AHOSTS, "w") as newActive:
			newList = ""
			for item in activeServers:
				newList += item + "\n"
			file.write(newList)


	# Communicates with all the chunkservers and requests the chunkhandles of all the chunks
	# residing on them. It then appends the locations of a chunk into the appropriate chunk object.
	def interrogateChunkServer(self, IP, retry):
		logging.debug('Initialize interrogateChunkServer()')

		# Create an instance of a TCP socket
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		# Define the variable that will hold the the data received from the chunkservers
		data = " "
		try:
			s.connect((IP, chunkPort))
			logging.debug('Connection Established: ' + str(IP) + ' on port ' + str(chunkPort))
			# Request the chunk contents of the specified chunkserver
			fL.send(s, 'CONTENTS?')
			logging.debug('Sent chunkserver a CONTENTS? message')
			# Received the chunk contents of the speicified chunkserver
			data = fL.recv(s)
			# Close the socket connection
			s.close()

		# If the database is unable to connect to the chunkservers, retry the connection
		# If the retry fails, remove it from the list of activehosts and move on.
		except:
			# Make sure the socket connection is closed before doing anything, so if 
			# a retry occurs, the socket will not already be in use.
			s.close()
			# heartBeat the chunkserver. If that indicates it is alive, try to 
			# interrogate the chunkserver again. If not, remove if from the active 
			# hosts list and move on.
			if retry < 3:
				# It the heartBeat indicated the chunkserver is still alive, try again.
				if hB.heartBeat(IP) == 1:
					self.intterogateChunkServer(IP, retry + 1)
					logging.debug('Retry connect to chunkserver for interrogation')
				else:
					self.remFromAhosts(IP)
					logging.warning('Heartbeat indicates chunkserver dead. Not interrogating, moving on.')
					return -1

			else:
				self.remFromAhosts(IP)
				# Log the fact that we were unable to connect to a chunkserver
				logging.error("interrogateChunkServer failed to connect to " + IP)
				return -1


		
		
		logging.debug('Received response from chunkserver')


		# If the IP is not already in the location lookup, add it!
		if IP not in self.locDict.keys():
			self.locDict[IP] = []


		# If the chunkserver has nothing on it, it should return whitespace. If this is the case, 
		# then nothing in the database can be updated, so it will continue onto the next IP.
		# If the chunkserver returns something other than whitespace (a message formatted chunk1|chunk2|... )
		# then the data can be processed.
		if data != " ":
			# Convert the pipe separated string into a list
			chunkData = data.split('|')
			# In the event that data is formatted poorly with additional | characters, we want to get rid of null elements
			chunkData = filter(None, chunkData)

			# For every chunk handle in that list, update that chunk objects locations list
			for chunk in chunkData:

				# If the IP key is not already in the location lookup, add it!
				if IP not in self.locDict.keys():
					self.locDict[IP] = []
					# Add the chunk to the list of values for the IP key
					self.locDict[IP].append(chunk)

				# If the location does already exist, append the current chunk to its list of chunk values
				else:
					assChnks = self.locDict[IP]
					# But first, make sure that chunk isn't already in the values, so you don't get
					# multiple copies of the same chunk in the lookup.
					if chunk not in assChnks:
						# Add the chunk to the list of values for the IP key
						self.locDict[IP].append(chunk)

				try:
					# Find which file the chunk is associated with in the lookup dictionary
					fileName = self.lookup[chunk]

					# From the file name we found the chunk to be associated with in the
					# lookup, we can append the current IP to the list of chunk locations
					# in the chunk object within the file object dictionary.
					self.data[fileName].chunks[chunk].locations.append(IP)
					logging.debug('Appended location to chunk ' + str(chunk) + ' location list')

				# If the chunk is not recognized in the master's database, the chunk is an orphan (does not
				# belong to a file). In this case, the chunk should be removed.
				except KeyError:
					# Create an instance of a TCP socket
					s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
					s.connect((IP, chunkPort))
					logging.debug('Connection Established: ' + str(IP) + ' on port ' + str(chunkPort))
					# Tell the chunkserver to sanitize the chunk
					fL.send(s, 'SANITIZE|' + str(chunk))
					logging.debug('Sent chunkserver a SANITIZE message')
					# Received the chunk contents of the speicified chunkserver
					data = fL.recv(s)

					if data == "SUCCESS":
						logging.debug("Orphan removal successful")
						self.locDict[IP].remove(chunk)
					elif data == "FAILED":
						logging.debug("Orphan removal failed.")
					# Close the socket connection
					s.close()
		

		logging.debug('interrogateChunkServer() complete')



	# This function should handle the event when a chunkserver is deemed to have left
	# the cluser. It updates all relevant metadata and initiates replication if needed.
	def chunkserverDeparture(self, IP):
		logging.debug("Begin chunkserverDeparture()")
		# Get the chunks that were stored at that location
		try:
			associatedChunks = self.locDict[IP]

		except KeyError:
			logging.error('The given IP was not found in the location dictionary. Unable to get metadata related to this location. Moving on..')
			# Set associatedChunks to be empty so it can continue through the function
			# and move on to the next IP
			associatedChunks = []


		logging.debug(associatedChunks)
		logging.debug("Got all chunks associated with the departed IP")
		# Go through all the chunks that were stored at that location and remove the IP
		# from it's list of locations.
		for chunk in associatedChunks:
			try:
				# Get the file the chunk belongs to
				fileName = self.lookup[chunk]
				logging.debug("file name lookup successful")

				# Remove the location from the chunk's location list
				self.data[fileName].chunks[chunk].locations.remove(IP)
				logging.debug("Old location removal successful")
				# Get a list of the places where the chunk is still stored.
				locs = self.data[fileName].chunks[chunk].locations
				logging.debug("Got list of chunk locations")
				# Get the length of the locations list
				lenLoc = len(locs)
				logging.debug("Found length of list")
				# Define the size of a chunk
				chunkSize = config.chunkSize

				# The byte offset should be 0, since we want to copy everything from the 
				# very beginning of the chunk to the very end.
				byteOffset = "0"


				# Make sure that the chunk is actually stored somewhere.
				if lenLoc == 0:
					logging.critical('FATAL: CHUNK ' + chunk + ' OF FILE ' + fileName + ' IS NO LONGER IN THE SYSTEM')
					# Do we want to have it remove the file from the database? Dead chunkservers may still have the chunks
					# on them, so we could wait until the chunkservers come back up and replicate? This goes along with
					# the question of what we do with orphan chunks..

				# Check to see if the chunk has less than three copies. If it has less than three, 
				# it needs to be replicated!
				elif lenLoc < 3:
					logging.debug("There are less than three copies of the chunk. Generating replicas...")
					# For as many replicas as need to be made
					for x in range(3-lenLoc):
						# Get a location where the new chunk will be put
						###########################################################
						# WHAT TO DO IF THERE ARE NO OTHER PLACES TO CHOOSE FROM? #
						###########################################################
						newLocation = self.chooseReplicaHost(locs)

						logging.debug("Successfully chose new loction: " + str(newLocation))
						# In the case where multiple replicas must be created, we want to make sure
						# we do not put the replicas in the same place, so we will add the new location
						# to the list of used locations
						locs.append(newLocation)
						logging.debug("Appended newLocation to list of locations")
						# Have a replica be created via the API
						flg = api.replicate(chunk, byteOffset, chunkSize, locs[0], newLocation)
						logging.debug("API call completed")
						# Check the flag to make sure the replication was successful
						if flg == 1:
							logging.debug("Flag == 1")
							# Update the database and locDict to reflect new chunk location
							self.data[fileName].chunks[chunk].locations.append(newLocation)

							logging.debug("locations updated in database data")
							# If the IP is not already in the location lookup, add it!
							if newLocation not in self.locDict.keys():
								self.locDict[newLocation] = []

							# Add the chunk to the list of values for the IP key
							self.locDict[newLocation].append(chunk)
							logging.debug("locations updated in locDict")

			except KeyError:
				logging.warning("key: " + str(chunk) + " does not exist. Probably an orphan chunk")



	def chooseReplicaHost(self, usedHosts):
		logging.debug("Beginning chooseReplicaHost()")
		try:
			# Get a list of all the hosts available
			with open(ACTIVEHOSTSFILE, 'r') as file:
				hostsList = file.read().splitlines()

			logging.debug("Successful parse of activehosts")

		except IOError:
			# Handle this error better in the future --> similar to how heartBeat.py
			# needs to handle for this case..
			logging.error( ACTIVEHOSTSFILE + ' does not exist')

		logging.debug("Removing hosts that already have the chunk")
		# Remove the IPs that already have a replica of the chunk on them
		for host in usedHosts:
			hostsList.remove(host)

		logging.debug("Getting the length of the host list")
		# Find how many unused hosts there are in the list
		lengthList = len(hostsList)

		################################################################
		# THIS WILL NEED TO BE UPDATED ONCE LOAD BALANCING IS IN PLACE #
		################################################################
		logging.debug("Randomizing..")
		# Randomize between the limits
		randomInt = random.randint(0, lengthList)

		# Return the randomized host
		return hostsList[randomInt%lengthList]




	# Gets a list of all the chunkhandles currently in use, then updates the chunkhandle
	# counter to the next unused number.
	def updateChunkCounter(self):
		# If the lookup is empty (no files/chunks in the file system), set the chunkHandle to 0
		if self.lookup.keys() == []:
			self.chunkHandle = 0
		else:
			# Get the max chunk handle from the database, and add one to get the current chunkhandle
			self.chunkHandle = int(max(self.lookup.keys())) + 1



	# createNewFile adds a new file key and file object to the database, and creates a new
	# empty chunk which can then be appended to.
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
			# Create a new chunk to be associated with the file just created
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

			# If the handleOfFullChunk is not the flag for a file create, -1, (where no latest
			# chunk should exist as the file was Just created), then find the chunk with the 
			# highest chunk handle (the chunk with the highest sequence number)
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
				locations = filter(None, locations)
				logging.debug(locations)

				#string to be returned
				string = ''
				# Append the chunkserver locations to the chunk's location list and update
				# the location-->chunk lookup
				for location in locations:
					self.locDict[location].append(chunkHandle)

					logging.debug('adding locations to new chunk')
					self.data[fileName].chunks[chunkHandle].locations.append(location)
					logging.debug('Appending locations to chunk ' + str(chunkHandle) + ' locations list')
					string += location+"|"
				logging.debug('file: ' + fileName + ' chunk: ' + str(self.data[fileName].chunks[chunkHandle]))


				# Add the chunk to the chunk/file lookup
				self.lookup[chunkHandle] = fileName

				logging.debug(self.data[fileName].chunks[chunkHandle].locations)

				# Update the opLog that a new chunk was created
				fL.appendToOpLog("CREATECHUNK|" + chunkHandle + "|" + fileName)
				string += chunkHandle
				#If this completed successfully, return the string to send.
				return string

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
		logging.debug("chunkHandle is " + chunkHandle)
		# Find the file name associated with the chunk
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


	# Gets a list of all the chunks associated with a given file
	def allChunks(self, fileName):
		# Get a list of all the chunkHandles associated with the file
		associatedChunks = self.data[fileName].chunks.keys()
		# Create an empty string to put all the chunks into
		msg = ""
		# Put all the chunks into a string
		for chunk in associatedChunks:
			msg += chunk + "|"

		return msg


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
		try:
			# Before we delete the file from the database, we want to make sure we know which chunks
			# are associated with it, so they can be removed from the lookup.
			associatedChunks = self.data[fileName].chunks.keys()
			# For each key, remove it from the lookup
			for chunk in associatedChunks:
				del self.lookup[str(chunk)]

			# We also want to make sure that it is no longer in the toDelete list, since it has been deleted
			if fileName in self.toDelete:
				self.toDelete.remove(fileName)

			# We want to remove it from the location-->chunk lookup as well. 
			# For all the keyed locations, we want to remove any chunk value that belongs to 
			# a file being deleted.
			for key in self.locDict:
				for chunk in associatedChunks:
					if chunk in self.locDict[key]:
						self.locDict[key].remove(chunk)

			# Delete the specified key/value pair from the database.
			del self.data[fileName]

			# Update the opLog that a new file was created
			fL.appendToOpLog("SANITIZED|-1|" + fileName)

			logging.debug('sanitizeFile() success')

		except KeyError:
			logging.error("The file to be deleted does not exist.")


	# A function that returns all of the file names that are currently in the database
	def getFiles(self):
		message = ''

		for fileName in self.data.keys():
			message += '|' + fileName + '*'

			for chunk in self.data[fileName].chunks.keys():
				message += chunk + '*'

				for location in self.data[fileName].chunks[chunk].locations:
					message += location + '*'

		return message
