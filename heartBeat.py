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
# Summary:      heartBeat.py sends a ping to each of the chunkservers in the    #
#		host.txt file. If the chunkserver responds, it adds it to an	#
#		activehosts file, so all responsive chunkserver addresses are	#
#		stored in one location. If the chunkserver does not respond, 	#
#		heartBeat.py makes sure it is removed from the activehosts list #
#                                                                               #
#################################################################################



import socket, time, os, config, logging, sys, listener
import functionLibrary as fL

#import debugging
fL.debug()





###############################################################################

#               Define heartBeat Object                                       #

###############################################################################


# Define a heartBeat object which will be used to ping the chunkservers and add those that
# respond to an activehosts log, while removing those that do not respond from the 
# activehosts log
class heartBeat:
	# Define variables that may need to be tweaked to optimize performance
	# SOCK_TIMEOUT sets the length of the timeout in seconds
	# PORT sets the port which the heartBeat will use to communicate with the chunkservers
	# DELAY sets the delay between chunkserver pings, as to reduce the load on the network
	SOCK_TIMEOUT = 2
	PORT = config.port
	DELAY = .25
	HOSTS = config.hostsfile
	AHOSTS = config.activehostsfile
	# Debug message for successful calling of heartBeat object
	logging.debug('init successful')

	# Function that returns a list of all chunk server IPs in the hosts.txt file
        def getChunkServerIPs(self):
        	# Debug message for successful calling of function
        	logging.debug('Getting chunk server IPs')
                # If the hosts.txt file exists:
                if os.path.isfile(self.HOSTS):
                        try:
                                # Read from it and parse its contents into a list. Return the list.
                                with open(self.HOSTS, "r") as file:
                                        cs = file.read().splitlines()
                                        # If there are any additional \n in the hosts file, this will
                                        # remove them from our list of chunkservers
                                        cs = filter(None, cs)
                                        return cs
                        # If the hosts.txt file can not be read, alert the logger
                        except IOError:
                                logging.error('Could not read from ' + self.HOSTS)
				listener.logError("Could not open or read from hosts file")
				exit(1)
                # If the hosts.txt file does not exist:
                else:
                        # TEMPORARY: print a message to inform of the issue (ISSUE #41)
                        logging.error(self.HOSTS + " does not exist.")
			listener.filesMissing()
			exit(1)

        # Function that returns a list of all chunk server IPs that are active 
        # (that have responded to heartbeats)
        def getActiveChunkServers(self):
        	# Debug message for successful calling of function
        	logging.debug('Getting active chunk server IPs')
                # If the activehosts.txt file exists:
                if os.path.isfile(self.AHOSTS):
                        try:
                                # Read from it and parse its contents into a list. Return the list.
                                with open(self.AHOSTS, "r") as file:
                                        activeCS = file.read().splitlines()
                                        return activeCS
                        # If the activehosts.txt file can not be read, alert the logger
                        except IOError:
                                # Retry opening/reading from file 
                                try:
                                        # Read from it and parse its contents into a list. Return the list.
                                        with open(self.AHOSTS, "r") as file:
                                              activeCS = file.read().splitlines()
                                              return activeCS
                                # If you are unable to read/open the file (permissions error?) then try to
                                # delete the file and create a new one.
                                except IOError:
                                        os.remove(self.AHOSTS)
                                        # Create a file called activehosts.txt
                                        open(self.AHOSTS, "a").close()
                                        # Return an empty list, as there are no contents yet in activehosts.txt
                                        # so parsing it into a list would return an empty list anyways.
                                        return []


                # If the activehosts.txt file does not exist:
                else:
                        # Create a file called activehosts.txt
                        open(self.AHOSTS, "a").close()
                        # Return an empty list, as there are no contents yet in activehosts.txt
                        # so parsing it into a list would return an empty list anyways.
                        return []

	# Function to ping chunk servers and, based on whether or not a response was received,
	# to add or remove them from the activehosts file
        def heartBeat(self, IP):
        	# Debug message for successful calling of function
        	logging.debug('Initiating heartBeat protocol')
                # Get the list of all active chunk server IPs
                activeServers = self.getActiveChunkServers()

                try:
                        # Create a TCP socket instance
                        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			# Set the timeout of the socket
                        self.s.settimeout(self.SOCK_TIMEOUT)
			# Allow the socket to re-use address
                        self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			# Connect to the chunkserver over the specified port
                        self.s.connect((IP, self.PORT))
			# Send the chunk server a heart (ping)
                        fL.send(self.s, "<3?")
			# Get the chunk server response
                        data = fL.recv(self.s)
                        print data
			# Close the connection to allow future connections
                        self.s.close()
                        # If the chunk server responds with a heart, add it to activehosts
                        if data == "<3!":
                                # If the chunkserver IP is not in the activeServers list, add it to the list
                                if IP not in activeServers:
                                        with open(self.AHOSTS, "a") as file:
                                                file.write(IP + "\n")
                        return 1
		# Handle the timeout (chunk server alive but not resonding) and connection (server dead) errors
		except (socket.timeout, socket.error):
			print "</3"
                        logging.debug('Could not connect to ' + IP)
			# Check to see if the chunk server is in the list of active IPs
                        if IP in activeServers:
				# If it is, remove it from the list
                                activeServers.remove(IP)
        			# Clear the previous activehosts.txt file and replace it with the list of active servers, 
        			# which now excludes the failed chunkserver
                                with open(self.AHOSTS, "w") as file:
                                        newList = ""
                                        for item in activeServers:
                                                newList += item + "\n"
                                        file.write(newList)
                        return -1


        # Makes sure that all the IPs in the active hosts still exist in the  
        # hosts file. Without this, if a server was removed from the hosts file,  
        # it would remain in the activehosts file. 
        def checkForMatch(self):
                # Get a list of all hosts
                chunkServers = self.getChunkServerIPs()
                # Get a list of all active hosts
                activeServers = self.getActiveChunkServers()
                # Define a list to hold the IPs that will need to be removed
                # from activehosts
                toRemove = []
                # Check to see if all things in activehosts exist in hosts. 
                # If there is something that is in active hosts but not in hosts, 
                # Add it to the list of IPs toRemove
                for item in activeServers:
                        if item not in chunkServers:
                                toRemove.append(item)
                # If there are things that need to be removed:
                if toRemove != []:      
                        # Remove the IPs from the activehosts list
                        for item in toRemove:
                                activeServers.remove(item)

                        # Define a string that will hold the new list of IPs
                        # excluding those that are not in the hosts file
                        activeList = ""
                        # Add all valid items to the activeList string
                        for item in activeServers:
                                activeList += item + "\n"
                        # Add the valid active IPs to the activehosts file, removing
                        # the invalid IPs
                        with open(self.AHOSTS, "w") as file:
                                file.write(activeList)


	# Function to iterate through all of the chunk servers and send a heart beat
	def pumpBlood(self):
		# Debug message for successful calling of function
                logging.debug('Begin sending heartbeats to chunk servers')
		# Get the list of chunk server IPs from the hosts file
                chunkServers = self.getChunkServerIPs()
		# For every IP, run the heartBeat funtion, with a short delay between each to ease network load
                for item in chunkServers:
			# Allows you to see which chunkserver it is currently communication with
                        print item
                        self.heartBeat(item)
                        time.sleep(self.DELAY)
                # Makes sure all the IPs in activehosts are valid host IPs
                self.checkForMatch()



###############################################################################

#               MAIN BODY                                                     #

###############################################################################

if __name__ == "__main__":
        # create an object instance and initiate the heartBeat
        master = heartBeat()
        master.pumpBlood()

