#!/usr/bin/env python

#########################################################################
#									#
#	manifetch.py securely copies the manifest from the master	#
#	server to the client						#
#									#
#########################################################################


import os

# Get the master server address from the HOSTS config file
with open("quesoFiesta.sh", "r") as hostfile:
	# Extract the config info stored at the head of quesoFiesta.sh
	HOST = hostfile.readlines()
	# Parse the list for the server information
	SERVER = HOST[2].split("=")
	SERVER = SERVER[1].strip()
	# Parse the list for the user information
	USER = HOST[3].split("=")
	USER = USER[1].strip()

# Securely copy the manifest from the master server to the /data/temp directory on the raspberry pi client
os.system("scp " + USER + "@" + SERVER + ":/data/gfsbin/manifest.txt /data/temp/manifest.txt")
