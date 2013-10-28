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
import socket
import threading
import time
import config

class API():

	#lets define some variables
	global MASTER_ADDRESS
	global TCP_PORT
	MASTER_ADDRESS = raw_input("What is the IP of the master? : ")
	TCP_PORT = config.port
	
	#lets make the API a server and a client to send and recieve messages
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
		self.s.send("CREATE|" + filename)
		self.data = self.s.recv(1024)
		print self.data
		#master sends back chunk handle and locations
		self.splitdata = self.data.split("|")
		dataLength = len(self.splitdata)
		cH = self.splitdata[-1]
		for n in range(0, dataLength-1):
			location = self.splitdata[n]
			#send this data to chunk servers
			thread = chunkConnectCreate(location, cH)
			thread.start()
		opLog = updateOpLog("OPLOG|CREATE|"+cH+"|"+filename)
		opLog.start()
	
	#appends to an existing file by first prompting the client for what 
	#new data they would like to add to the file (the filename is given 
	#as an arg). The API sends append and the filename to the master which
	#sends back the chunk handle and locations of the existing file. The 
	#client then sends "append" and the new data to the chunk servers which
	#append the new data to the files.
	def append(self, filename):
		global newData
		newData = str(raw_input("Input the data you want to append: "))
		#appends to an existing file
		self.s.send("APPEND|" + filename)
		self.data = self.s.recv(1024)
		print self.data
		#gets data from master
		self.splitdata = self.data.split("|")
		dataLength = len(self.splitdata)
                cH = self.splitdata[-1]
                for n in range(0, dataLength-1):
                        location = self.splitdata[n]
	                #send this data to chunk servers
                        thread = chunkConnectAppend(location, cH, newData)
                        thread.start()

               	

#thread for create
class chunkConnectCreate(threading.Thread):
	def __init__(self, location, cH):
		threading.Thread.__init__(self)
		self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.location = location
		self.cH = cH
		
	def run(self):
		self.s.connect((self.location,TCP_PORT))
		print "makeChunk"
		self.s.send("makeChunk")
		dat = self.s.recv(1024)
		if dat == "continue":	
			print self.cH
			self.s.send(self.cH)

#thread for append
class chunkConnectAppend(threading.Thread):
        def __init__(self, location, cH, newData):
                threading.Thread.__init__(self)
		self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.location = location
                self.cH = cH
              
        def run(self):
                self.s.connect((self.location,TCP_PORT))
                self.s.send("append")
		print "append"
		dat = self.s.recv(1024)
               	if dat == "continue":
			self.s.send(self.cH)
			print self.cH
		dat = self.s.recv(1024)
		if dat == "continue":
			self.s.send(newData)
			print newData

class updateOpLog(threading.Thread):
        def __init__(self, message):
                threading.Thread.__init__(self)
                self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.message = message

        def run(self):
                self.s.connect((MASTER_ADDRESS,TCP_PORT))
                self.s.send(self.message)
		print "opshit"


API = API()
API.create("/foob/bar")


