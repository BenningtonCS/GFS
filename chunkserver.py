#################################################################~
#        10/23/2013                                                     #~~
#        Multi-Threaded Chunk Server                                  #~~~
#                by                                                   #~~~~
#        Torrent Glenn & Brendon Walter                              #~~~~
#                                                                     #~~~~~
#                                                                      #~~~~~~
########################################################################~~~~~~~

""" The Chunk Server is a multithreaded server designed to run on multiple 
server machines it uses the socket library for connections with the other 
members of the GFS; the threading library to ensure that it can handle multiple 
requests; and the os library to manage miscellaneous file counting functions. 
The main thread listens on the specified port and upon accepting a connection 
spawns a distributor thread which parses the first message and hands the 
connection to the appropriate worker thread""" 

#import all the necessary libraries
import socket, threading, os, config, sys, logging, datetime
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
        logging.basicConfig(filename='chunkserverLog.log', format='%(asctime)s %(levelname)s : %(message)s')






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

class workerThread(connThread):
	daemon = True
	def __init__(self,acceptedConn): # it has no data option because its going 
				         # to make/get the data
		threading.Thread.__init__(self) # call threading.Thread.__init__ 
						# in order to initialize the thread 
						# correctly
		self.connection = acceptedConn[0] # give itself the accepted connection
		self.remoteAddress = acceptedConn[1] 

	def run(self):
		command = self.connection.recv(1024) # listens for a command on 
							# the connection handed 
							# down from the main 
							# thread

		# next the distributor hands the connection to the appropriate 
		# worker based on the command given, invalid commands simply fall 
		# through

		if command == "<3?":
			# in this and each other if/elif statement the correct 
			# worker thread is started for a given command
			self.connection.send("<3!")
			self.connection.close()

		elif command == "CHUNKSPACE?":
			self.connection.send("continue") # after receiving the connection 
							 # the thread confirms that it is 
							 # ready to receive arguments
			chunkHandle = self.connection.recv(1024) # it listens on its 
								 # connection for a chunkhandle
			emptySpace = mg64 - os.stat(chunkHandle).st_size # then checks the 
								         # difference 
								         # between the 
								         # file's size and 
								         # 64mg (the max 
								         # chunk size)
			self.connection.send(emptySpace) # and returns the amount of space 
							 # left to the API
			self.connection.close() # closes the connection

		elif command == "READ":
			self.connection.send("continue") # confirms readiness for data
			chunkHandle = self.connection.recv(1024) # listens for chunkHandle
			self.connection.send("continue") # confirms ready state
			byteOffSet = int(self.connection.recv(1024)) # listens for a byte 
								     # offset to read from 
								     # (relative to the 
								     # beginning of the 
								     # given chunk)
			self.connection.send("continue") # confirms the desire for EVEN MORE data
			bytesToRead = int(self.connection.recv(1024)) # listens for the 
								      # number of bytes to read
			chunk = open(config.chunkPath+"/"+chunkHandle) # opens the designated chunk to read from
			chunk.seek(byteOffSet) # goes to the specified byte offset
			fileContent = chunk.read(bytesToRead) # stuffs all the stuff to be 
							      # read into a variable
			self.connection.send(fileContent)
			chunk.close() # closes the chunk
			self.connection.close() # closes the connection

		elif command == "CONTENTS?":
			files = []
            for filenames in os.walk(self.path): # read every file
            	files.append(filenames)      # append each one to a list
                output = str( '|'.join(files[0][2])) # turn the list into a string
			if output == "":		     # if there is nothing in the dir
				self.connection.send(" ")    # send an empty string
			else:				     # otherwise
				self.connection.send(output) # send everything as a string

		elif command == "CREATE":
			self.connection.send("continue")
            chunkHandle = self.connection.recv(1024) # get the name of the chunk
            open(chunkPath+"/"+chunkHandle, 'w').close() # create the file

		elif command == "APPEND":
			self.connection.send("continue")
			chunkHandle = self.connection.recv(1024) # name of the chunk
			self.connection.send("continue") 
			data = self.connection.recv(config.chunkSize)    # data being added
                with open(config.chunkPath+"/"+chunkHandle, 'a') as a: # open the chunk
                        a.write(data) 			 # add the data to it

 		else:
 			error = "Received invalid command: " + command
 			logging.error(error)


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # set up the socket for some 
# set the reuseaddr option in order to not clog up the port
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
s.bind((ADDRESS, PORT)) # bind

while 1: # always and forever
	s.listen(1) # listen for incoming connections from the master or API
	t = workerThread(s.accept()) # if something comes in spawn a 
					  # distributor thread and hand it off
	t.start() # start the distributor thread
s.close()
