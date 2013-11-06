#!/usr/bin/env python


##############################################################################
#                        ################                                    #
#                        # API Version 1#                                    #
#                        ################                                    #
#This is the API version 1. It currently has the create and append functions #
#with read, open, close, and delete to be implemented later on (soon). This  #
#file will be imported and called from a client file that the client can just#
#run.                                                                        #
##############################################################################


#import socket for connection, threading to make threads, time in case we want
#a delay, and config to keep the protocol standard.
import socket, threading, time, config, sys, logging
import functionLibrary as fL



###############################################################################

#               Verbose (Debug) Handling                                      #

###############################################################################


# Setup for having a verbose mode for debugging:
# USAGE: When running program, $python API.py , no debug message will show up
# Instead, the program should be run in verbose, $python API.py -v , for debug 
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
        logging.basicConfig(filename='apiLog.log', format='%(asctime)s %(levelname)s : %(message)s')








class API():

	#lets define some variables
	global MASTER_ADDRESS
	global TCP_PORT
	MASTER_ADDRESS = config.masterip
	TCP_PORT = config.port

	#lets make the API able to send and recieve messages
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 	
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	s.connect((MASTER_ADDRESS, TCP_PORT))
	

	#lets make some methods
	
	#creates a file by first sending a request to the master. Then the 
	#master will send back a chunk handle followed by three locations in
	#which to create this chunk handle. the client then sends the chunk 
	#handle to the three locations (which are chunk servers) along with
	#the data "makeChunk". The chunkservers then make an empty chunk at
	#each of those locations. Takes the filename as an arguement.
	def create(self,filename):
		#creates a file and gives it to the master
		fL.send(self.s, "CREATE|" + filename)
		self.data = fL.recv(self.s)
		if self.data == "FAIL":
			print "THAT FILE EXISTS ALREADY... EXITING API"
			exit(0)
		#print self.data
		#master sends back chunk handle and locations
		self.splitdata = self.data.split("|")
		dataLength = len(self.splitdata)
		cH = self.splitdata[-1]
		#spawn threads that connect to the chunk servers
		for n in range(0, dataLength-1):
			self.s.close()
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			location = self.splitdata[n]
			print location
			#send this data to chunk servers
			s.connect((location,TCP_PORT))
                	fL.send(s, "CREATE")
                	print "CREATE"
                	dat = fL.recv(s)
                	if dat == "CONTINUE":   
                        	print cH
                        	fL.send(s, cH)
			#thread = chunkConnectCreate(location, cH)
			#thread.start()
		s.close()
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect((MASTER_ADDRESS, TCP_PORT))

		opLog = updateOpLog("OPLOG|CREATECHUNK|"+cH+"|"+filename)
		opLog.start()
	
	#appends to an existing file by first prompting the client for what 
	#new data they would like to add to the file (the filename is given 
	#as an arg). The API sends append and the filename to the master which
	#sends back the chunk handle and locations of the existing file. The 
	#client then sends "append" and the new data to the chunk servers which
	#append the new data to the files.
	def append(self, filename, newData):
		#appends to an existing file
		fL.send(self.s, "APPEND|" + filename)
		self.data = fL.recv(self.s)
		print self.data
		#gets data from master
		self.splitdata = self.data.split("|")
		dataLength = len(self.splitdata)
                cH = self.splitdata[-1]
		#spawn threads that connect to the chunk servers
                for n in range(0, dataLength-1):
			self.s.close()
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        location = self.splitdata[n]
	                #send this data to chunk servers
			self.s.connect((location,TCP_PORT))
                	fL.send(s, "APPEND")
                	print "APPEND"
                	dat = fL.recv(s)
                	if dat == "CONTINUE":
                        	fL.send(s, cH)
                        	print cH
                	dat = fL.recv(s)
                	if dat == "CONTINUE":
                        	fL.send(self.s, self.newData)
                        	print self.newData
                        #thread = chunkConnectAppend(location, cH, newData)
                        #thread.start()
		s.close()
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((MASTER_ADDRESS, TCP_PORT))

	#reads an existing file by taking th filename, byte offset, and the number of bytes the client
	#wants to read as args. This information is passed on to the master which sends back a list
	#where every element is a list. The outer list is a list of all the chunks that one copy of 
	#the file is on and the inner lists are the locations of each chunk and har far to read on
	#that chunk. I then pass on the necessary data to the chunk servers which send me back the
	#contents of the file. 
	def read(self, filename, byteOffset, bytesToRead):
		#send read and the filename to the master
		fL.send(self.s, "READ|" + filename + "|" + str(byteOffset) + "|" + str(bytesToRead))
		#recieve data from the master
		self.data = fL.recv(self.s)
		print self.data
		#split the data into a list
		self.splitdata = self.data.split("|")
		self.splitdata = self.splitdata[1:]
		print self.splitdata
		for q in self.splitdata:
			secondSplit = q.split("*")
			print secondSplit
			location = secondSplit[0]
			print "location = ", location
			cH = secondSplit[1]
			print "cH = ", cH
			offset = secondSplit[2]
			print "offset = ", offset
			s.close()
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s.connect((self.location,TCP_PORT))
                	fL.send(s, "READ")
                	print "READ"
                	dat = fL.recv(s)
                	if dat == "CONTINUE":
                        	fL.send(s, cH)
                        	print cH
                	dat = fL.recv(s)
                	if dat == "CONTINUE":
                        	fL.send(s, self.offSet)
                        	print self.offSet
                	dat = fL.recv(s)
                	if dat == "CONTINUE":
                        	fL.send(s, self.bytesToRead)
                        	print self.bytesToRead
                	dat = fL.recv(s)
                	print dat
			#thread = chunkConnectRead(location, cH, offset, bytesToRead)
			#thread.start()	
               	s.close()
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((MASTER_ADDRESS, TCP_PORT))

	def fileList(self):
		fL.send(self.s, "FILELIST|x")
		self.data = fL.recv(s)
		return self.data

#thread for create
"""class chunkConnectCreate(threading.Thread):
	def __init__(self, location, cH):
		threading.Thread.__init__(self)
		self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.location = location
		self.cH = cH
		
	def run(self):
		self.s.connect((self.location,TCP_PORT))
		fL.send(self.s, "makeChunk")
		print "makeChunk"
		dat = fL.recv(self.s)
		if dat == "continue":	
			print self.cH
			fL.send(self.s, self.cH)"""

#thread for append
'''class chunkConnectAppend(threading.Thread):
        def __init__(self, location, cH, newData):
                threading.Thread.__init__(self)
		self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.location = location
                self.cH = cH
		self.newData = newData
              
        def run(self):
                self.s.connect((self.location,TCP_PORT))
                fL.send(self.s, "APPEND")
		print "APPEND"
		dat = fL.recv(self.s)
               	if dat == "continue":
			fL.send(self.s, self.cH)
			print self.cH
		dat = fL.recv(self.s)
		if dat == "continue":
			fL.send(self.s, self.newData)
			print self.newData'''

#thread for read
"""class chunkConnectRead(threading.Thread):
	def __init__(self, location, cH, offSet, bytesToRead):
		threading.Thread.__init__(self)
		self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.location = location
		self.cH = cH
		self.offSet = offSet
		self.bytesToRead = bytesToRead
	def run(self):
		self.s.connect((self.location,TCP_PORT))
		fL.send(self.s, "READ")
		print "READ"
		dat = fL.recv(self.s)
		if dat == "continue":
			fL.send(self.s, self.cH)
			print self.cH
		dat = fL.recv(self.s)
		if dat == "continue":
			fL.send(self.s, self.offSet)
			print self.offSet
		dat = fL.recv(self.s)
		if dat == "continue":
			fL.send(self.s, self.bytesToRead)
			print self.bytesToRead
		dat = fL.recv(self.s)
		print dat"""		

#oplog stuff. for questions contact rohail
class updateOpLog(threading.Thread):
        def __init__(self, message):
                threading.Thread.__init__(self)
                self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.message = message

        def run(self):
                self.s.connect((MASTER_ADDRESS,TCP_PORT))
                fL.send(self.s, self.message)
		print "opLog message sent"



