import functionLibrary as fL
import config, logging

###############################################################################

#               Verbose (Debug) Handling                        

###############################################################################


# Setup for having a verbose mode for debugging:
# USAGE: When running program, $python database.py , no debug message will show up
# Instead, the program should be run in verbose, $python database.py -v , for debug 
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
        logging.basicConfig(filename='masterLog.log', format='%(asctime)s DATABASE %(levelname)s : %(message)s')


###############################################################################

#               OBJECT DEFINITIONS                

###############################################################################

class File:
	def __init__(self, name):
		self.fileName = name
		self.chunks = {}
		self.delete = False

class Chunk:
	def __init__(self):
		self.location = []



class Database:
	# Create an empty dictionary to store the chunks, keyed to the file name
	data = {}

	# Create an empty dictionary to be used as a chunk --> fileName lookup
	lookup = {}

	def initialize(self):

		########### BREAK THIS INTO A READ FROM OPLOG FUNCTION LATER ##########
		with open(OPLOG, 'r') as oplog:
			opLog = oplog.read().splitlines()

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

			# If the operation was to create a chunk, create a new chunk object and 
			# add it to the database
			elif lineData[0] == 'CREATECHUNK':
				# Create a new instance of the Chunk object
				chunk = Chunk()
				# In the file object associated with the file name the chunk belongs to,
				# add the newly created chunk to the chunk dictionary, where the chunk
				# object is the value and the chunk handle is the key
				self.data[lineData[2]].chunks[lineData[1]] = chunk
				# Update the lookup dictionary
				self.lookup[lineData[1]] = lineData[2]

			# If the operation was to delete a file, change the file object's delete attribute
			# to True, so the scrubber will recognize it as marked for deletion.
			elif lineData[0] == 'DELETE':
				# Flag the given file for deletion
				self.data[lineData[2]].delete = True

			# If the operation was to undelete a file, change the file object's delete attribute
			# back to False, so the scrubber will not delete it.
			elif lineData[0] == "UNDELETE":
				# Unflag the given file for deletion
				self.data[lineData[2]].delete = False

			# If the operation was to sanitize, that is, the chunks were actually deleted,
			# as opposed to marked for deletion, then remove the metadata for the file and
			# associated chunks from the database
			elif lineData[0] == "SANITIZED":
				# Delete the specified key/value pair
				del self.data[lineData[2]]



		########### BREAK THIS INTO A CHUNKSERVER INTERROGATION FUNCTION LATER ##########
		hostFile = open(ACTIVEHOSTSFILE)

		with open(ACTIVEHOSTSFILE, "r") as hosts:
			hostList = hosts.read().splitlines()

		for IP in hostList:
			# COULD USE A TRY/EXCEPT IN HERE PROBABLY IN CASE THE CONNECTION DOES NOT WORK
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			s.connect((IP, chunkPort))
			fL.send(s, 'CONTENTS?')
			data = fL.recv(s)

			chunkData = data.split('|')

			for chunk in chunkData:
				# ADD SOME ERROR HANDLING HERE -- IF THE CHUNK DOES NOT EXIST IN THE 
				# LOOKUP SOMETHING WENT TERRIBLY WRONG!
				fileName = lookup[chunk]

				# From the file name we found the chunk to be associated with in the
				# lookup, we can append the current IP to the list of chunk locations
				# in the chunk object within the file object dictionary.
				self.data[fileName].chunks[chunk].locations.append(IP)



















