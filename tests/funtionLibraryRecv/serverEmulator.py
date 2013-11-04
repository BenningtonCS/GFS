#!/usr/bin/env python

#################################################################
#										
#	SERVER EMULATOR FOR FUNCTIONLIBRARY TESTING		
#						
#################################################################

import socket, config
import functionLibrary as fL

port = int(raw_input('What port to connect over? : '))

# Create a server TCP socket and allow address re-use
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

# Bind the listener-server 
s.bind(('', port))


ending = str(raw_input('Include proper EOT character in response? (y/n): ')).lower()
# Listen for client connections
s.listen(1)
print "Listening..."
# Accept the incoming connection
conn, addr = s.accept()
print "accepted connection"
# Receive the data
data = fL.recv(conn)

# If data was received
if data:
	print data
	# If the option to send back an eot was yes, send the message
	# with the eot
	if ending == "y":
		fL.send(conn, "Received successfully")
	# If the option to send back an eot was no, send the message
	# without the eot
	elif ending == "n":
		conn.send("Received successfully")

# If no data was received
if not data:
	print "No data received!"
	# If the option to send back an eot was yes, send the message
	# with the eot
	if ending == "y":
		fL.send(conn, "Received unsuccessfully")
	# If the option to send back an eot was no, send the message
	# without the eot
	elif ending == "n":
		conn.send("Received unsuccessfully")
