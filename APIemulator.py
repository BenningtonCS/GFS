#!/usr/bin/env python

# API CLIENT USED FOR MASTER TESTING PURPOSES

# Acts as a pseudo-API that sends the master a valid request, receives
# the message back from the master, and instead of acting any further
# upon the received message, it will print the message to console, so 
# we can verify that the correct message has been received.


import socket
import functionLibrary as fL

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


# Initialze the socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

# Establish a TCP connection
s.connect((ADDRESS, PORT))

# Format the CREATE and APPEND messages properly, and then send to master
if op == "CREATE" or op =="APPEND":
	message = op + "|" + fileName
	fL.send(s, message)
	print 'sent: \"' + message + '\"'
# Format the READ message properly and then send to master
elif op =="READ":
	message = op + "|" + fileName + "|" + byteOffset + "|" + bytesToRead
	fL.send(s, message)
	print 'sent: \"' + message + '\"'
# If the command was an unexpected command, let the user know!
else:
	print "NOT A VALID COMMAND"

# Receive a response from the master
data = fL.recv(s)
# Print the master's response
print data

# Close the TCP connection
s.close()

