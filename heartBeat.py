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



import socket, time, os, config, logging, sys
import functionLibrary as fL



###############################################################################

#               Verbose (Debug) Handling                                      #

###############################################################################


# Setup for having a verbose mode for debugging:
# USAGE: When running program, $python heartBeat.py , no debug message will show up
# Instead, the program should be run in verbose, $python heartBeat.py -v , for debug 
# messages to show up

# Get a list of command line arguments
args = sys.argv
FORMAT = "%(asctime)s %(levelname)s : %(message)s"
# Check to see if the verbose flag was one of the command line arguments
if "-v" in args:
        # If it was one of the arguments, set the logging level to debug 
        logging.basicConfig(level=logging.DEBUG, format=FORMAT)
else:
        # If it was not, set the logging level to default (only shows messages with level
        # warning or higher)
        logging.basicConfig(filename='masterLog.log', format=FORMAT)




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
                                        return cs
                        # If the hosts.txt file can not be read, alert the logger
                        except IOError:
                                logging.error('Could not read from ' + self.HOSTS)
                # If the hosts.txt file does not exist:
                else:
                        # TEMPORARY: print a message to inform of the issue (ISSUE #41)
                        logging.error(self.HOSTS + " does not exist.")

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
                                logging.error('Could not read from ' + self.AHOSTS)
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



###############################################################################

#               MAIN BODY                                                     #

###############################################################################

if __name__ == "__main__":
        # create an object instance and initiate the heartBeat
        master = heartBeat()
        master.pumpBlood()

