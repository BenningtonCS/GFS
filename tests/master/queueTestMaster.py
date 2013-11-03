#!/usr/bin/env python

import socket, threading, random, os, time, Queue





# Define constructor which will build an API-handling thread
class handleCommand(threading.Thread):
	# Define initialization parameters
	def __init__(self, ip, port, socket, data, lock):
		threading.Thread.__init__(self)
		self.ip = ip
		self.port = port
		self.s = socket
		self.data = data
		self.lock = lock


	# Funtion to parse input into usable data by splitting at a pipe character
	def handleInput(self, apiInput):
		# Create a list by splitting at a pipe
		input = apiInput.split("|")
		# Return the list
		return input


	# Function that executes the protocol when a CREATE message is received
	def create(self):
		print "sending create"
		self.s.send("ip1|ip2|ip3|chunkHandle")
		print "sent create"


	# Function that executes the protocol when an APPEND message is received
	def append(self):
		print "sending append"
		self.s.send("loc1|loc2|loc3|chunkHandle")
		print "sent append"


	# Function that executes the protocol when a READ message is received
	def read(self):
		print "sending read"
		self.s.send("READFROM|ip1*chunkHandle*ByteOffset*endOffset")
		print "sent read"


	# Function to handle the message received from the API
	def run(self):
		# Parse the input into the msg variable
		self.msg = self.handleInput(self.data)
		# Define the first item in the list to be the operation name
		self.op = self.msg[0]


		# If the operation is to CREATE:
		if self.op == "CREATE":
			self.create()

		# If the operation is to APPEND:
		elif self.op == "APPEND":
			self.append()

		# If the operation is to READ:
		elif self.op == "READ":
			self.read()

						
		else:
			# If the operation is something else, something went terribly wrong. 
			# This error handling needs to vastly improve
			logging.error("Command " + self.op + " not recognized. No actions taken.")

	def __del__(self):
		print "deleting object"



def worker():
	print "worker init"
	while True:
		item = q.get()
		print "got from queue, about to run fn"
		handler = handleCommand(addr[0], addr[1], conn, data, threadLock)
		handler.run()
		print "ran handleCommand"
		q.task_done()
		print "marked done"



#######################################################################

#                       MAIN 						                  #

#######################################################################

# Define the paths of the host file, activehost file, and oplog from the config file, and
# define the port to be used, also from the config file
chunkPort = 9666
print chunkPort
# Define a thread lock to be used to get and increment chunk handles
threadLock = threading.Lock()
q = Queue.Queue(maxsize=0)

WORKERS = 5
print "WORKERS"

for i in range(WORKERS):
	t = threading.Thread(target=worker)
	t.daemon = True
	t.start()

print "queue for loop"

# Create a server TCP socket and allow address re-use
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

print "socketsetup"

# Bind the listener-server 
s.bind(('', chunkPort))
print "bound"

while 1:
	try:
		# Listen for API connections
		s.listen(1)
		print "Listening..."
		# Accept the incoming connection
		conn, addr = s.accept()
		print "accept"
		# Receive the data
		data = conn.recv(1024)
		print data
		q.put(data)
		print "put"
		# When the connection is established and data is successfully acquired,
		# start a new thread to handle the command. Having this threaded allows for
		# multiple commands (or multiple API) to interact with the master at one time
		#newThread = handleCommand(addr[0], addr[1], conn, data, threadLock)
		#newThread.start()

	# If someone ends the master through keyboard interrupt, break out of the loop
	# to allow the threads to finsh before closing the main thread
	except KeyboardInterrupt:
		print "Exiting Now."
		break






