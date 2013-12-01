#!/usr/bin/env python


##############################################################################
#                        ################                                    #
#                        # API Version 1#                                    #
#                        ################                                    #
#This is the API version 1. It currently has the create, append, read, delete#
#and undelete functions. Open and close will be added if desired.            #
#file will be imported and called from a client file that the client can just#
#run.                                                                        #
##############################################################################


#import socket for connection, threading to make threads, time in case we want
#a delay, and config to keep the protocol standard.
import socket, threading, time, config, sys, logging, struct, os
import functionLibrary as fL

fL.debug()

class API():

	#lets define some variables
	global MASTER_ADDRESS
	global TCP_PORT
	MASTER_ADDRESS = config.masterip
	TCP_PORT = config.port

	#lets make some methods
	
	#creates a file by first sending a request to the master. Then the 
	#master will send back a chunk handle followed by three locations in
	#which to create this chunk handle. the client then sends the chunk 
	#handle to the three locations (which are chunk servers) along with
	#the data "CREATE". The chunkservers then make an empty chunk at
	#each of those locations. Takes the filename as an argument.
	def create(self,filename):
		#return an error if some wise guy tries to put a pipe in the file name.
		if "|" in filename:
			print "Invalid character in filename. Please retry with a different filename. exiting..."
			exit(0)

		logging.debug("API: Starting create function.")
		#lets make the API able to send and recieve messages
		logging.debug("API: Creating socket.")
		m = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        	m.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        	try:
                	m.connect((MASTER_ADDRESS, TCP_PORT))
        	except:
                	logging.error("ERROR: COULD NOT CONNECT TO MASTER")
                	exit(0)

		#send a CREATE request to the master
		try:
			logging.debug("API: Attempting to send CREATE| " + filename)
			fL.send(m, "CREATE|" + filename)
		except: 
			logging.error("ERROR: COULD NOT SEND CREATE REQUEST TO MASTER")
		#receive data back from the master 
		self.data = fL.recv(m)
		#error if the file trying to be created already exists 
		logging.debug("API: Received message: " + self.data)
		if self.data == "FAIL1":
			print "THAT FILE EXISTS ALREADY... EXITING API"
			exit(0)
		elif self.data == "FAIL2":
			print "NO SUCH FILE EXISTS FOR CHUNK CREATION"
			exit(0)
		elif self.data == "FAIL3":
			print "CHUNK IS NOT THE LATEST CHUNK"
			exit(0)
		print self.data
		#parse the received data into locations, and chunk handle
		self.splitdata = self.data.split("|")
		dataLength = len(self.splitdata)
		cH = self.splitdata[-1]
		global ack
		logging.debug("API: about to begin for loop, " + str(dataLength -1) + "iterations")
		#iterate through each IP address received from the master
		for n in range(0, dataLength-1):
			logging.debug("API: For loop, iteration number " + str(n))
			#create a socket to be used to connect to chunk server
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			#designate the IP for this iteration
			location = self.splitdata[n]
			print location
			#attempt to connect to the chunk server at the current location
			try:
				s.connect((location,TCP_PORT))
                	except: 
				logging.error("ERROR: COULD NOT CONNECT TO CHUNKSERVER AT ", location)
				continue
			#send CREATE request to the chunk server at the current location
			fL.send(s, "CREATE|" + cH)
			print "CREATE"
			#wait to receive a CONTINUE from chunk server to proceed
			global ack
			ack = fL.recv(s)
			#close connection to current chunk server.
			s.close()

		if ack == "FAILED":
			print "ERROR: FILE CREATION FAILED"
#			fL.send(m, "FAILED")
		elif ack == "CREATED":
			print "File creation successful!"
#				fL.send(m, "CREATED")
			return 1
		m.close()
		
		#oplog stuff for questions contact rohail
	#	try:
	#		opLog = updateOpLog("OPLOG|CREATE|"+cH+"|"+filename)
	#		opLog.start()
	#	except:
	#		print "COULD NOT UPDATE OPLOG"

	
	#appends to an existing file by first prompting the client for what 
	#new data they would like to add to the file (the filename is given 
	#as an arg). The API sends append and the filename to the master which
	#sends back the chunk handle and locations of the existing file. The 
	#client then sends "append" and the new data to the chunk servers which
	#append the new data to the files.
	def append(self, filename, newData,flag):
		#lets make the API able to send and recieve messages
	        m = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        	m.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        	try:
                	m.connect((MASTER_ADDRESS, TCP_PORT))
        	except:
                	print "ERROR: COULD NOT CONNECT TO MASTER"
                	exit(0)
		#send APPEND request to master
		try:
			fL.send(m, "APPEND|" + filename)
		except:
			print "COULD NOT SEND APPEND REQUEST TO MASTER"
			exit(0)
		#receive data back from master
		self.data = fL.recv(m)
		print self.data
		#some error handling
		if (self.data == "FAILED"):
			print "ERROR: MASTER SENT FAIL MESSAGE exiting..."
			exit(0)
		elif (self.data == "OPEN"):
			print "ERROR: FILE " +filename+" ALREADY OPEN"
			return 0
		#parse the data into useful parts
		self.splitdata = self.data.split("|")
		dataLength = len(self.splitdata)
                cH = self.splitdata[-1]
		#get length of the requested new data to use for append across chunks
		"""if flag:
			dataSize = os.path.getsize(newData)
			strct = struct.Struct(str(dataSize)+"s")
			newData = strct.pack((open(newData,"rb").read()))"""
		if flag:
			with open(newData,"rb") as da:
				newData = da.read()
			
			
	
		dataSize = len(newData)
		
		print dataSize
		
		lenNewData = int(dataSize)
		#close connection to master 
        	m.close()
	        for n in range(0, dataLength-1):
			#create socket to connect to chunk server at location
			self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        location = self.splitdata[n]
	                #attempt to connect to chunk server at location
			try:
				self.s.connect((location,TCP_PORT))
                	except:
				print "ERROR: COULD NOT CONNECT TO CHUNK SERVER AT ", location
			#ask chunk server how much room is left on latest chunk
			fL.send(self.s, "CHUNKSPACE?|" + cH)
			#the response is stored in remainingSpace
			remainingSpace = fL.recv(self.s)
			#some error handling
			if remainingSpace == "FAILED":
				print "CHUNKSPACE REQUEST FAILED. exiting..."
				exit(0)
			#make remainingSpace an integer
			remainingSpace = int(remainingSpace)
			print lenNewData
			print remainingSpace
			self.s.close()
			#if the length of the new data is greater than the room left in the chunk...
			if (lenNewData > remainingSpace):   
				#...split the data into two parts, the first part being equal to the
				#amount of room left in the current chunk. the second part being the 
				#rest of the data.
				cut = remainingSpace
                                newData1 = newData[0:cut]
				print "Sending data of length:" + str(len(newData1))
                                newData2 = newData[cut:]
			#	print newData2
				#tell the chunk server to append the first part of the new data that
				#will fill up the rest of the remaining space on a chunk
				s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				try:
					s.connect((location, TCP_PORT))
				except:
					print "ERROR: COULD NOT REOPEN SOCKET"
				fL.send(s, "APPEND|" + cH + "|" + newData1)
				print "first append"
				SoF = fL.recv(s)
				#error handling
				if SoF == "FAILED":
					print "ERROR WITH APPEND ON CHUNK SERVER SIDE. exiting..."
					exit(0)
				#close connection to chunk server
				s.close()
				
			elif (lenNewData <= remainingSpace):
				t = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				t.connect((location, TCP_PORT))
				try:
					fL.send(t, "APPEND|" + cH + "|" + newData)
				except:
					print "ERROR: COULD NOT SEND APPEND TO CHUNK SERVER"			
				SoF = fL.recv(t)
				#error handling/acks
				if SoF == "FAILED":
					print "ERROR WITH APPEND ON CHUNK SERVER SIDE. exiting..."
					exit(0)

		###################
		if lenNewData > remainingSpace:
			m  = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        try:
                        	m.connect((MASTER_ADDRESS, TCP_PORT))
				print "connected to master"
                        except:
                                print "ERROR: COULD NOT CONNECT TO MASTER DURING APPEND ACROSS CHUNKS"
                                exit(0)
			#tell the master to create a new chunk for the remaining data
			try:
				fL.send(m, "CREATECHUNK|" + filename + "|" + cH)
			except:
				print "ERROR: COULD NOT CREATE NEW CHUNK TO APPEND TO"
			#receive data back from master
			cData = fL.recv(m)
			#parse this data and handle it very similarly as the in the create function
			if self.data == "FAIL2":
				print "NO SUCH FILE EXISTS FOR CHUNK CREATION"
				exit(0)
			splitcData = cData.split("|")
			cDataLength = len(splitcData)
                	cH = splitcData[-1]
                	#close the connection to the master so we can connect to the chunk servers
                	#m.close()
                	#iterate through each IP address received from the master
                	for n in range(0, cDataLength-1):
                        	#create a socket to be used to connect to chunk server
                        	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        	#designate the IP for this iteration
                        	location = splitcData[n]
                        	print location
                        	#attempt to connect to the chunk server at the current location
                        	try:
                                	s.connect((location,TCP_PORT))
                        	except:
                                	print "ERROR: COULD NOT CONNECT TO CHUNKSERVER AT ", location
                                	continue
                        	#send CREATE request to the chunk server at the current location
                        	fL.send(s, "CREATE|" + cH)
				global ack
                        	ack = fL.recv(s)
                ################  	
                        #close connection to current chunk server.
	                        s.close()

				#do some acks
	                	if ack == "FAILED":
	                        	print "ERROR: CHUNK CREATION FAILED"
#	                        	fL.send(m, "FAILED")
	                	elif ack == "CREATED":
	                        	print "Chunk creation successful!"
#	                        	fL.send(m, "CREATED")
	               		m.close()
			
			#now that the new chunk has been created on all of the servers...
			#...run append again with the second part of the new data
			#self.s.close()
			try:
				self.append(filename, newData2,False)
			except UnboundLocalError:
				pass
		return 1

	#reads an existing file by taking the filename, byte offset, and the number of bytes the client
	#wants to read as args. This information is passed on to the master which sends back a list
	#where every element is a list. The outer list is a list of all the chunks that one copy of 
	#the file is on and the inner lists are the locations of each chunk and har far to read on
	#that chunk. I then pass on the necessary data to the chunk servers which send me back the
	#contents of the file. 
	def read(self, filename, byteOffset, bytesToRead, newName):
		#lets make the API able to send and recieve messages
        	m = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        	m.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        	try:
                	m.connect((MASTER_ADDRESS, TCP_PORT))
        	except:
                	print "ERROR: COULD NOT CONNECT TO MASTER"
                	exit(0)
		#send READ request to the master
		try:
			fL.send(m, "READ|" + filename + "|" + str(byteOffset) + "|" + str(bytesToRead))
		except:
			print "ERROR: COULD NOT SEND READ REQUEST TO MASTER"
		#recieve data from the master
		self.data = fL.recv(m)
		print self.data
		#split the data into a list
		self.splitdata = self.data.split("|")
		#remove the first element of the list because it is irrelevant
		self.splitdata = self.splitdata[1:]
		#close connection to master
		m.close()
		#iterate through the list
		fromChunks = ""
		fileContents = ""
		for q in self.splitdata:
			#split the list into smaller parts
			secondSplit = q.split("*")
			#print secondSplit
			#set the location...
			location = secondSplit[0]
			#print "location = ", location
			#...and the chunk handle
			cH = secondSplit[1]
			#print "cH = ", cH
			#...and the offset
			offset = secondSplit[2]
			#print "offset = ", offset
			#close connection to master
			#connect to the chunk server
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			try:
				s.connect((location,TCP_PORT))
			except:
				print "ERROR: COULD NOT CONNECT TO CHUNK SERVER AT ", location
				exit(0)
			#send READ request to chunk server
                	fL.send(s, "READ|" + str(cH) + "|" + str(offset) + "|" + str(bytesToRead))
                	#print "READ|" + cH + "|" + offset + "|" + bytesToRead
			#receive and print the contents of the file
			#fromChunks += "." + str(cH)
                	dat = fL.recv(s)
			print str(len(dat)) + "from" + location
                #	fileContents += dat
					
		#close connection to chunk server		
	               	s.close()
#              	strct = struct.Struct(str(len(fileContents))+"s")
               	
			with open(newName,"ab") as e:
				e.write(dat)
				e.close()
		
		return 1
		#reestablish connection to master


	#This is the delete function. It takes a filename as a parameter and 
	#deletes the given file from our GFS implementation. When a DELETE 
	#request is sent to the master it marks the file for deletion. The 
	#next time the garbage collector runs it will remove any marked files
	def delete(self, filename):
		#lets make the API able to send and recieve messages
        	m = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        	m.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        	try:
                	m.connect((MASTER_ADDRESS, TCP_PORT))
        	except:
                	print "ERROR: COULD NOT CONNECT TO MASTER"
                	exit(0)
		#send DELETE request to the master
		try:
			fL.send(m, "DELETE|" + filename)
		except:
			print "ERROR: COULD NOT SEND DELETE REQUEST TO MASTER"
		#receive acks from the master
		self.data = fL.recv(m)
		#tell the user whether the file was successfully marked or not
		if self.data == "FAILED1":
			print "ERROR: The file could not be marked for deletion."
			return -1
		elif self.data == "FAILED2":
			print "ERROR: The given file name does not exist."
			return -2
		elif self.data == "FAILED3":
			print "The file has already been marked for deletion."
			return -3
		elif self.data == "MARKED":
			print "File successfully marked for deletion."
		m.close()
	
	#This is the undelete function. It takes a filename as a parameter and 
	#if that file is marked for deletion, and the garbage collector has not 
	#removed it yet, the file will be unmarked and safe from deletion.
	def undelete(self, filename):
		#lets make the API able to send and recieve messages
        	m = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        	m.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        	try:
                	m.connect((MASTER_ADDRESS, TCP_PORT))
        	except:
                	print "ERROR: COULD NOT CONNECT TO MASTER"
                	exit(0)
		#send UNDELETE request to master
		try:
			fL.send(m, "UNDELETE|" + filename)
		except:
			print "ERROR COULD NOT SEND UNDELETE REQUEST TO MASTER"
		#receive acks from the master
		self.data = fL.recv(m)
		#tell the user whether the file was successfully unmarked or not
		if self.data == "FAILED1":
			print "ERROR: COULD NOT UNDELETE FILE"
			return -1
		elif self.data == "FAILED2":
			print "File was not flagged for deletion."
			return -2
		elif self.data == "MARKED":
			print "File successfully unmarked for deletion."
		m.close()
	

	def fileList(self):
                #lets make the API able to send and recieve messages
                m = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                m.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try:
                        m.connect((MASTER_ADDRESS, TCP_PORT))
                except:
                        print "ERROR: COULD NOT CONNECT TO MASTER"
                        exit(0)

                try:
                        fL.send(m, "FILELIST|x")
                        data = fL.recv(m)
                        m.close()
                        return data
                except:
                        print "file list error"

#oplog stuff. for questions contact rohail
class updateOpLog(threading.Thread):
        def __init__(self, message):
                threading.Thread.__init__(self)
                self.m = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.message = message

        def run(self):
                self.m.connect((MASTER_ADDRESS,TCP_PORT))
                fL.send(self.m, self.message)
		print "opLog message sent"
		m.close()


