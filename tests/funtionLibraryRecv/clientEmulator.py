#!/usr/bin/env python

#################################################################
#																#
#	CLIENT EMULATOR FOR FUNCTIONLIBRARY.RECV() TESTING			#
#																#
#################################################################

import socket, config
import functionLibrary as fL

port = int(raw_input('What port to connect over? : '))

# Define a socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# Allow socket address reuse
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
# Connect to the server over the specified IP and port 
s.connect(('', port))


message = str(raw_input('What message would you like to send? : '))
ending = str(raw_input('Include proper EOT character? (y/n): ')).lower()

# Send the message with a proper EOT termination
if ending == "y":
	s.send(message + config.eot)

# Send the message without the proper EOT termination
elif ending == "n":
	s.send(message)

# Receive data back from the server
data = fL.recv(s)

# Print the data to console
print "DATA == ", data


s.close()
