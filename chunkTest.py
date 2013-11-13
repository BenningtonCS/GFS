import threading, os, socket, config, sys
import functionLibrary as fL 



PORT = config.port
remotePi = sys.argv[1]
threadNum = int(sys.argv[2])
role = int(sys.argv[3])
appendData = "rawr"
chunkHandle = sys.argv[4]
byteOffSet = "1"
bytesToRead = "5"


class testThread(threading.Thread):
	daemon = True
	def __init__(self,port,remoteAddr,role):
		threading.Thread.__init__(self)
		self.port = port
		self.remoteAddr = remoteAddr
		self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.role = role
		
	
	def run(self):
		
		if self.role == 1:
			print "1"
			self.socket.connect((self.remoteAddr,self.port))
			print "2"
			fL.send(self.socket, "<3?")
			print "3"
			print fL.recv(self.socket)
			print "4"

		elif self.role == 2:
			self.socket.connect((self.remoteAddr,self.port))
			fL.send(self.socket, "CHUNKSPACE?|"+chunkHandle)
			print "chunkspace :" + fL.recv(self.socket)

		elif self.role == 3:
			self.socket.connect((self.remoteAddr,self.port))
			fL.send(self.socket, "READ|"+chunkHandle+"|"+byteOffSet+"|"+bytesToRead)
			print "Reading.... " + fL.recv(self.socket)


		elif self.role == 4:
			self.socket.connect((self.remoteAddr,self.port))
			fL.send(self.socket, "CONTENTS?")
			print "contents :" + fL.recv(self.socket)

		elif self.role == 5:
			self.socket.connect((self.remoteAddr,self.port))
			fL.send(self.socket, "CREATE|"+chunkHandle)
			print fL.recv(self.socket)


		elif self.role == 6:
			self.socket.connect((self.remoteAddr,self.port))
			fL.send(self.socket, "APPEND|"+chunkHandle+"|"+appendData)

		elif self.role == 7:
			self.socket.connect((self.remoteAddr,self.port))
			fL.send(self.socket, "SANITIZE|"+chunkHandle)
			print fL.recv(self.socket)



for i in range(threadNum):
	t = testThread(PORT,remotePi,role)
	t.start()
	t.join()

