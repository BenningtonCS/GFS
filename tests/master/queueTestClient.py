#!/usr/bin/env python

# CLIENT USED TO TEST THE QUEUE FUNCTIONALITY OF THE MASTER

# Acts as a simple API which would only send one command, but a lot of that
# one command to make sure the master's queue functionality is working as 
# it should be.


import socket, time

# Define the address and port to connect over
ADDRESS = ''
PORT = 9666

# Get user input for what the API emulator should send
op = raw_input("what operation? (CREATE, READ, APPEND) : ")
fileName = raw_input("what is the file name? : ")
# In the case that the user wants to READ, get the additional required input
# from the user
if op == "READ":
	byteOffset = raw_input("what byte offset to start reading from? : ")
	bytesToRead = raw_input("how many bytes to read? : ")




for x in range(0,1000):
	# Initialze the socket
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	# Establish a TCP connection
	s.connect((ADDRESS, PORT))

	if op == "CREATE" or op =="APPEND":
		s.send(op + "|" + fileName)
		print "sent!"
	# Format the READ message properly and then send to master
	elif op =="READ":
		s.send(op + "|" + fileName + "|" + byteOffset + "|" + bytesToRead)
	# If the command was an unexpected command, let the user know!
	else:
		print "NOT A VALID COMMAND"

	# Receive a response from the master
	data = s.recv(1024)
	# Print the master's response
	print data
	# Wait a second for visibility
	time.sleep(1)

	# Close the TCP connection
	s.close()




