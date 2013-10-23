#!/usr/bin/env python

#API version 1

import socket
import threading
import time

class API():

	#lets define some variables
	global MASTER_ADDRESS
	global TCP_PORT
	MASTER_ADDRESS = '10.10.117.109'
	TCP_PORT = 9666
	MY_ADDRESS = '10.10.117.182'
	
	#lets make the API a server and a client to send and recieve messages
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 	
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	s.connect((MASTER_ADDRESS, TCP_PORT))
	

	#lets make some methods
	
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
		#self.s.send("OPLOG|CREATE|"+cH+"|"+filename)
		#self.s.send("CREATE|" + filename)

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


