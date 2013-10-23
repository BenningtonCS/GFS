#!/usr/bin/env python

#################################################################################
#                                                                               #
#               GFS Distributed File System HeartBeat				#
#_______________________________________________________________________________#
#                                                                               #
# Authors:      Erick Daniszewski                                               #
#                                                                               #
# Date:         21 October 2013                                                 #
# File:         heartBeat.py                                                    #
#                                                                               #
# Summary:      Send love over TCP		                                #
#                                                                               #
#################################################################################

import socket, time, os

# Define a heartBeat object
class heartBeat:
	# Define constants that may want to be changed later
	SOCK_TIMEOUT = 2
	PORT = 9666
	DELAY = 3

	# Function that returns a list of all chunk server IPs in the HOSTS.txt file
        def getChunkServerIPs(self):
                # If the HOSTS.txt file exists:
                if os.path.isfile("hosts.txt"):
                        # Read from it and parse its contents into a list. Return the list.
                        with open("hosts.txt", "r") as file:
                                cs = file.read().splitlines()
                                return cs
                # If the HOSTS.txt file does not exist:
                else:
                        # Print a vulgar message because it should always exist.
                        print "Fuck! Abort! Abort!"

        # Function that returns a list of all chunk server IPs that are active (that have responded to heartbeats
        def getActiveChunkServers(self):
                # If the activehosts.txt file exists:
                if os.path.isfile("activehosts.txt"):
                        # Read from it and parse its contents into a list. Return the list.
                        with open("activehosts.txt", "r") as file:
                                activeCS = file.read().splitlines()
                                return activeCS
                # If the activehosts.txt file does not exist:
                else:
                        # Create a file called activehosts.txt
                        open("activehosts.txt", "a").close()
                        # Return an empty list, as there are no contents yet in activehosts.txt
                        return []

	# Function to check if chunk servers are active or not
        def heartBeat(self, IP):
                # Get the list of all chunk server IPs
                chunkServers = self.getChunkServerIPs()
#               print chunkServers
                # Get the list of all active chunk server IPs
                activeServers = self.getActiveChunkServers()
#               print activeServers

                try:
                        # Create a TCP socket instance
                        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			# Set the timeout of the socket
                        self.s.settimeout(self.SOCK_TIMEOUT)
			# Allow the socket to re-use address/port
                        self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			# Connect to the chunkserver over a specified port
                        self.s.connect((IP, self.PORT))
			# Send the chunk server a heart
                        self.s.send("<3?")
			# Get the chunk server response
                        data = self.s.recv(1024)
                        print data
			# Don't forget to close the connection so future heartBeats have a chance!
                        self.s.close()
                        # If the chunk server sends back a heart:
                        if data == "<3!":
                                # If the chunkserver IP is not in the activeServers list, add it to the list
                                if IP not in activeServers:
                                        with open("activehosts.txt", "a") as file:
                                                file.write(IP + "\n")
		# Handle the timeout (chunk server alive but not resonding) and connection (server dead) errors
		except (socket.timeout, socket.error):
			print "</3"
			# Check to see if the chunk server is in the list of active IPs
                        if IP in activeServers:
				# If it is, remove it from the list
                                activeServers.remove(IP)
			# Clear/create the activehosts.txt file and put in it the list of active servers, which now excludes the failed chunkserver
                        with open("activehosts.txt", "w") as file:
                                newList = ""
                                for item in activeServers:
                                        newList += item + "\n"
                                file.write(newList)

	# For all the chunkservers in the HOSTS list, send a heartbeat to see if it is still alive
	def pumpSomeBlood(self):
		# Get the list of chunk server IPs from the HOSTS file
                chunkServers = self.getChunkServerIPs()
		# For every IP, run the heartBeat funtion, with a short delay between each to ease network load
                for item in chunkServers:
			# Allows you to see which chunkserver it is currently communication with
                        print item
                        self.heartBeat(item)
                        time.sleep(self.DELAY)

# initiate!
master = heartBeat()
master.pumpSomeBlood()

