#################################################################~
#        10/23/2013            		                         #~~
#	Multi-Threaded Chunk Server	                          #~~~
#		by				                   #~~~~
#	Torrent Glenn & Brendon Walter                              #~~~~
#				                                     #~~~~~
#			                                              #~~~~~~
########################################################################~~~~~~~

""" The Chunk Server is a multithreaded server designed to run on multiple 
server machines it uses the socket library for connections with the other 
members of the GFS; the threading library to ensure that it can handle multiple 
requests; and the os library to manage miscellaneous file counting functions. 
The main thread listens on the specified port and upon accepting a connection 
spawns a distributor thread which parses the first message and hands the 
connection to the appropriate worker thread""" 

#import all the necessary libraries
import socket, threading, os, config, sys, logging
import functionLibrary as fL


###############################################################################

#               Verbose (Debug) Handling                                      #

###############################################################################


# Setup for having a verbose mode for debugging:
# USAGE: When running program, $python chunkserver.py , no debug message will 
# show up. Instead, the program should be run in verbose, $python chunkserver.py -v , 
# for debug messages to show up

# Get a list of command line arguments
args = sys.argv
# Check to see if the verbose flag was one of the command line arguments
if "-v" in args:
        # If it was one of the arguments, set the logging level to debug 
        logging.basicConfig(level=logging.DEBUG, format='%(levelname)s : %(message)s')
else:
        # If it was not, set the logging level to default (only shows messages 
	# with level warning or higher)
        logging.basicConfig(filename='chunkserverLog.txt', format='%(asctime)s %(levelname)s : %(message)s')






mg64 = 1024*1024 # 64 megabytes in binary
	
ADDRESS = '' # set address to the local IP
PORT = config.port # carry out all communications on port 9666

class connThread(threading.Thread): 
	# This class is the parent class from which all other threads inherit 
	# it gives all its child threads connection capabilities
	daemon = True
	def __init__(self,acceptedConn,data=0): # optional data slot in addition
				  		# to accepted connection
		threading.Thread.__init__(self)
		self.connection = acceptedConn[0] # in the __init__ function the 
						  # accepted connection is split 
						  # into connection..
		self.remoteAddress = acceptedConn[1] # ... and a remote address
		self.data = data 

class heartBeatThread(connThread):
	# The heartBeatThread just sends a "<3!" to the master, it is called with 
	# the command "<3?"
	def run(self):
		fL.send(self.connection, "<3!")
		self.connection.close()

class chunkSpaceThread(connThread):
	# This is activated by the "chunkSpace?" command. 
	def run(self):
		fL.send(self.connection, "continue") # after receiving the connection
						     # the thread confirms that it is 
						     # ready to receive arguments
		chunkHandle = fL.recv(self.connection) # it listens on its connection 
						       # for a chunkhandle
		emptySpace = mg64 - os.stat(chunkHandle).st_size # then checks the 
						       # difference between the file's 
						       # size and 64mg (the max chunk size)
		fL.send(self.connection, emptySpace) # and returns the amount of space left 
						     # to the API
		self.connection.close() # closes the connection

class chunkReaderThread(connThread):
	# activated by the "Read" command.
	def run(self):
		fL.send(self.connection, "continue") # confirms readiness for data
		chunkHandle = fL.recv(self.connection) # listens for chunkHandle
		fL.send(self.connection, "continue") # confirms ready state
		byteOffSet = int(fL.recv(self.connection)) # listens for a byte offset to
							   # read from (relative to the 
							   # beginning of the given chunk)
		fL.send(self.connection, "continue") # confirms the desire for EVEN MORE data
		bytesToRead = int(fL.recv(self.connection)) # listens for the number of bytes
							    # to read
		chunk = open("chunkHandle") # opens the designated chunk to read from
		chunk.seek(byteOffSet) # goes to the specified byte offset
		fileContent = chunk.read(bytesToRead) # stuffs all the stuff to be read into
						      # a variable
		fL.send(self.connection, fileContent)
		chunk.close() # closes the chunk
		self.connection.close() # closes the connection

################################ Entering Brendon's Code #######################
class onPi(connThread):
	# onPi reads every file within the path (which should be /data/gfsbin/Chunks)
	# and returns it as a list in the form of <chunk handle>|<chunk handle>| etc.
	path = config.chunkPath
        
        def run(self):
                files = []
                for filenames in os.walk(self.path): # read every file
                        files.append(filenames)      # append each one to a list
                output = str( '|'.join(files[0][2])) # turn the list into a string
		if output == "":		     # if there is nothing in the dir
			fL.send(self.connection, " ")    # send an empty string
		else:				     # otherwise
			fL.send(self.connection, output) # send everything as a string
                
class makeChunk(connThread):
	# makeChunk creates an empty file that has the name of the chunkhandle that 
	# was given to it.
        def run(self):
		fL.send(self.connection, "continue")
                chunkHandle = fL.recv(self.connection) # get the name of the chunk
                open(path+chunkHandle, 'w').close() # create the file
class appendChunk(connThread):
	# appendChunk adds data that is handed to it to the given chunkhandle.
        def run(self):
		fL.send(self.connection, "continue")
		chunkHandle = fL.recv(self.connection) # name of the chunk
		fL.send(self.connection, "continue") 
		data = fL.recv(self.connection)    # data being added
                with open(path+chunkHandle, 'a') as a: # open the chunk
                        a.write(data) 			 # add the data to it

########################## Exiting Brendon's Code #############################

class distributorThread(connThread):
	# The mighty distributor thread
	def __init__(self,acceptedConn): # it has no data option because its 
					 # going to make/get the data
		threading.Thread.__init__(self) # call threading.Thread.__init__ 
						# in order to initial the thread 
						# correctly
		self.connection = acceptedConn # give itself the accepted connection
	def run(self):
		command = fL.recv(self.connection[0]) # listens for a command on 
						      # the connection handed down 
						      # from the main thread
		# next the distributor hands the connection to the appropriate worker
		# based on the command given, invalid commands simply fall through

		if command == "<3?":
			# in this and each other if/elif statement the correct 
			# worker thread is started for a given command
			beat = heartBeatThread(self.connection)
			beat.start()
		elif command == "CHUNKSPACE?":
			t = chunkSpaceThread(self.connection)
			t.start()
		elif command == "READ":
			t = chunkReaderThread(self.connection)
			t.start()
		elif command == "CONTENTS?":
			t = onPi(self.connection)
			t.start()
		elif command == "MAKECHUNK":
			t = makeChunk(self.connection)
			t.start()
		elif command == "APPEND":
			t = appendChunk(self.connection)
			t.start()
	
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # set up the socket for 
						      # some TCP action
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # set the reuseaddr 
							# option in order to 
							# not clog up the port
s.bind((ADDRESS, PORT)) # bind

while 1: # always and forever
	s.listen(1) # listen for incoming connections from the master or API
	t = distributorThread(s.accept()) # if something comes in spawn a 
					  # distributor thread and hand it off
	t.start() # start the distributor thread
s.close()

