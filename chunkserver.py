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
#import debugging
fL.chunkdebug()



mg64 = config.chunkSize # 64 megabytes in binary
chunkPath = config.chunkPath # get the chunk path prefix from config file
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
		c = fL.recv(self.connection) # listens for a command on 
							# the connection handed 
							# down from the main 
							# thread
		com = c.split('|') # if c has multiple parts, they will be seperated by
				   # pipes. This will put each part into a list
		command = com[0]   # the command should be the first part of the message,
				   # thus the first part of the list
		logging.debug("Recieved command " + command)

		# next the distributor hands the connection to the appropriate 
		# worker based on the command given, invalid commands simply fall 
		# through

		if command == "<3?":
			# heartbeat to see if each chunkserver is running. If it is, it will
			# send back a confirmation of <3!
			try:
				# in this and each other if/elif statement the correct 
				# worker thread is started for a given command
				fL.send(self.connection, "<3!")
				logging.debug("Send heart beat back")
				self.connection.close()
				logging.debug("Closed connection.")
			except socket.error as e:
				logging.error(e)

		elif command == "CHUNKSPACE?":
			try:
#				fL.send(self.connection, "CONTINUE") # after receiving the connection 
#								 # the thread confirms that it is 
#								 # ready to receive arguments
#				logging.debug("send a continue")
#				chunkHandle = fL.recv(self.connection) # it listens on its 
#									 # connection for a chunkhandle
				chunkHandle = com[1] # name of the chunkhandle
				logging.debug("recieved name of the chunkhandle: " + chunkHandle)
				emptySpace = str(mg64 - os.stat(chunkPath + "/" + chunkHandle).st_size) # then checks the 
									         # difference 
									         # between the 
									         # file's size and 
									         # 64mg (the max 
									         # chunk size)
				fL.send(self.connection, str(emptySpace)) # and returns the amount of space 
								 # left to the API
				logging.debug("Send the space remaining")
#				self.connection.close() # closes the connection
#				logging.debug("Closed the connection")
			except socket.error as e:
				logging.error(e)
			except IOError as e:
				fL.send(self.connection,"FAILED")
				logging.error(e)
			except Exception as e:
				fL.send(self.connection,"FAILED")
				logging.error(e)

		elif command == "READ":
			# read data from a chunk
			try:
#				fL.send(self.connection, "CONTINUE") # confirms readiness for data
#				logging.debug("sent continue #1")
#				chunkHandle = fL.recv(self.connection) # listens for chunkHandle
				chunkHandle = com[1]
#				logging.debug("recieved name of the chunkhandle: " + chunkHandle)
#				fL.send(self.connection, "CONTINUE") # confirms ready state
#				logging.debug("sent continue #2")
				byteOffSet = int(com[2]) # listens for a byte 
									     # offset to read from 
									     # (relative to the 
									     # beginning of the 
									     # given chunk)
#				byteOffSet 
#				logging.debug("recieved the byte offset number.")
#				fL.send(self.connection, "CONTINUE") # confirms the desire for EVEN MORE data
#				logging.debug("sent continue #3") 
				bytesToRead = int(com[3]) #int(fL.recv(self.connection)) # listens for the 
									      # number of bytes to read
				logging.debug("recieved the number of bytes to read")
				chunk = open(chunkPath+"/"+chunkHandle) # opens the designated chunk to read from
				chunk.seek(int(byteOffSet)) # goes to the specified byte offset
				fileContent = chunk.read(bytesToRead) # stuffs all the stuff to be 
								      # read into a variable
				fL.send(self.connection, fileContent)
				logging.debug("send the file content")
				chunk.close() # closes the chunk
				logging.debug("closed the connection")
				self.connection.close() # closes the connection
			except socket.error as e:
				logging.error(e)
			except IOError as e:
				fL.send(self.connection,"FAILED")
				logging.error(e)
			except Exception as e:
				fL.send(self.connection,"FAILED")
				logging.error(e)

		elif command == "CONTENTS?":
			try:
				output = ""
				for files in os.walk(chunkPath): # read every file
					output = str('|'.join(item for item in files[-1])) # turn the list into a string
					print output
				if output == "":		     # if there is nothing in the dir
					print "output is empty"
					fL.send(self.connection, " ")    # send an empty string
					logging.debug("Sent an empty string which should be the output")
				else:				     # otherwise
					print "output is not empty"
					fL.send(self.connection, output) # send everything as a string
					logging.debug("sent the output")
			except socket.error as e:
				logging.error(e)
			except IOError as e:
				fL.send(self.connection,"FAILED")
				logging.error(e)
			except Exception as e:
				fL.send(self.connection,"FAILED")
				logging.error(e)

		elif command == "CREATE":
			# create a new chunk
			try:
#				fL.send(self.connection, "CONTINUE")
#				logging.debug("Sent continue")
	                	chunkHandle = com[1] #fL.recv(self.connection) # get the name of the chunk
				logging.debug("recieved name of the chunk")
	                	open(chunkPath + "/" + chunkHandle, 'w').close() # create the file
	        	except IOError as e:
				logging.error(e)
				fL.send(self.connection, "FAILED")
			except Exception as e:
				logging.error(e)
				fL.send(self.connection, "FAILED")
			else:
				fL.send(self.connection, "CREATED")
			

		elif command == "APPEND":	
			# append new data to a chunk
			try:
#				fL.send(self.connection, "CONTINUE")
#				logging.debug("sent continue #1")
#				chunkHandle = fL.recv(self.connection) # name of the chunk
				chunkHandle = com[1]
#				logging.debug("Recieved name of the chunk")
#				fL.send(self.connection, "CONTINUE") 
#				logging.debug("Sent continue #2") 
#				data = fL.recv(self.connection)    # data being added
				data = "|".join(com[2:])
				length = str(len(data))
				logging.error(length)
				logging.debug("Recieved the data to be added")
	                	with open(chunkPath+"/"+chunkHandle, 'a') as a: # open the chunk
	                        	#for item in data:
					a.write(data)		 # add the data to it
	                except socket.error as e:
				logging.error(e)
			except IOError as e:
				fL.send(self.connection,"FAILED")
				logging.error(e)
			except Exception as e:
				fL.send(self.connection,"FAILED")
				logging.error(e)
			else:
				fL.send(self.connection,"SUCCESS")

		elif command == "SANITIZE":
			# recieves SANITIZE from the scrubber which tells the chunkserver to delete a chunk
			try:
				chunkHandle = com[1] # name of the chunk
				logging.debug("Recieved name of the chunk")
				try:
					os.remove(chunkPath + '/' + chunkHandle) # remove file from directory
					logging.debug("Removed chunk handle.")
				# If there is an error thrown that the chunk does not exist, we return success
				# because sanitize is called to remove a chunk from a location. If it does not
				# exist at that location, then removal is technially achieved.
				except OSError:
					fL.send(self.connection, "SUCCESS")
					logging.debug("chunk already did not exist. sending success")
					
				fL.send(self.connection, "SUCCESS") # send a success
				logging.debug("removal successfull!")
				
			except socket.error as e:
				logging.error(e)
			except IOError as e:
				fL.send(self.connection,"FAILED")
				logging.error(e)
			except Exception as e:
				fL.send(self.connection,"FAILED")
				logging.error(e)



 		else:
 			error = "Received invalid command: " + command
 			logging.error(error)


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # set up the socket for some 
# set the reuseaddr option in order to not clog up the port
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
s.bind((ADDRESS, PORT)) # bind

logging.info("Chunkserver initialized")
while 1: # always and forever
	s.listen(1) # listen for incoming connections from the master or API
	logging.debug("listening for a connection")
	t = workerThread(s.accept()) # if something comes in spawn a 
					  # distributor thread and hand it off
	logging.debug("accepted connection. Created thread.")
	t.start() # start the distributor thread
s.close()