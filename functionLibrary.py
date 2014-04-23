#!/usr/bin/env

#################################################################################
#										
#		GFS COMMON FUNCTION LIBRARY 					
#_______________________________________________________________________________
#																
# Date:		30 October 2013							
# File:		functionLibrary.py 						
#										
# Summary:	The function library is a library that all components of the GFS
#		implementation have access to, which contains basic functions that	
#		are used by multiple components, so the same code need not be 	
#		written repeatedly. 						
#										
#################################################################################


import config,sys,logging,socket,random,struct,os


###############################################################################

#               DEFINE SOME CONSTANTS                                        #

###############################################################################


# From the config file, get the end of transmission character
eot = config.eot
HOSTSFILE = config.hostsfile
ACTIVEHOSTSFILE = config.activehostsfile
OPLOG = config.oplog
chunkPort = config.port
eotlen = len(eot)
###############################################################################

#               Verbose (Debug) Handling                                      #

###############################################################################

# Setup for having a verbose mode for debugging:
# USAGE: When running program, $python API.py , no debug message will show up
# Instead, the program should be run in verbose, $python API.py -v , for debug 
# messages to show up

def debug():
	# Get a list of command line arguments
	# Check to see if the verbose flag was one of the command line arguments
	if "-v" in sys.argv:
	        # If it was one of the arguments, set the logging level to debug 
	        logging.basicConfig(level=logging.DEBUG, format='%(levelname)s : %(message)s')
	else:
	        # If it was not, set the logging level to default (only shows messages with level
	        # warning or higher)
	        logging.basicConfig(filename='masterLog.log', level=logging.INFO, format='%(asctime)s %(levelname)s : %(message)s')


def chunkdebug():
	# Get a list of command line arguments
	# Check to see if the verbose flag was one of the command line arguments
	if "-v" in sys.argv:
	        # If it was one of the arguments, set the logging level to debug 
	        logging.basicConfig(level=logging.DEBUG, format='%(levelname)s : %(message)s')
	else:
	        # If it was not, set the logging level to default (only shows messages with level
	        # warning or higher)
	        logging.basicConfig(filename='httpServerFiles/chunkserverLog.log', level=logging.INFO, format='%(asctime)s %(levelname)s : %(message)s')


###############################################################################

#               TCP RECEIVE FUNCTION 

###############################################################################

packer = struct.Struct('!L')

def recv(connection):
        msg = ""
        msgLen = packer.unpack(connection.recv(4))[0]
#	msgLen = int(connection.recv(8))
        while len(msg) < msgLen:
                msg += connection.recv(msgLen-len(msg))
        return msg

###############################################################################

#               TCP SEND FUNCTION 

###############################################################################

def send(connection, message):
	totalSent = 0
	msgLen = len(message)
	packed_msgLen = packer.pack(msgLen)
	connection.send(packed_msgLen)
	while totalSent < msgLen:
		sent = connection.send(message[totalSent:])
		totalSent += sent

###############################################################################

#               RANDOMLY CHOOSE CHUNK SERVER LOCATIONS

###############################################################################


def chooseHosts():
	# Visual confirmation for debugging: confirm init of chooseHosts()
	logging.debug('Selecting storage locations')

	try:
		# Get a list of all the hosts available
		with open(ACTIVEHOSTSFILE, 'r') as file:
			hostsList = file.read().splitlines()
		# Find how many hosts there are in the list
		lengthList = len(hostsList)
		# Randomize between the limits
		randomInt = random.randint(0, lengthList)
		# Visual confirmation for debugging: confirm success of chooseHosts()
		logging.debug('Successfully selected storage locations')
		# Return a pipe-seperated list of randomized hosts
		return hostsList[randomInt%lengthList] + "|" + hostsList[(randomInt + 1)%lengthList] + "|" + hostsList[(randomInt + 2)%lengthList]

	except IOError:
		# Handle this error better in the future --> similar to how heartBeat.py
		# needs to handle for this case..
		logging.error( ACTIVEHOSTSFILE + ' does not exist')




###############################################################################

#               APPEND TO OPLOG

###############################################################################


# When called, this function will append the specified data into the oplog. Data 
# passed in should follow the format <operation>|<chunkHandle>|<filename>
def appendToOpLog(data):
	try:
		# Append the given data into the oplog file
		with open(OPLOG, 'a') as oplog:
			oplog.write(data + "\n")

	# If the oplog is unable to be opened or appended to, retry, if that fails, alert the listener
	except IOError:
		try:
			# Append the given data into the oplog file
			with open(OPLOG, 'a') as oplog:
				oplog.write(data + "\n")

		except IOError:
			logging.error("Could not append to: " + OPLOG)
			listener.logInfo('FATAL', 'Appending to opLog was unsuccessful. Database integrity at risk.')



###############################################################################

#               GET IP

###############################################################################

import fcntl
import struct

def get_interface_ip(ifname):
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	return socket.inet_ntoa(fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s',ifname[:15]))[20:24])

def get_lan_ip():
    ip = socket.gethostbyname(socket.gethostname())
    if ip.startswith("127.") and os.name != "nt":
        interfaces = [
            "eth0",
            "eth1",
            "eth2",
            "wlan0",
            "wlan1",
            "wifi0",
            "ath0",
            "ath1",
            "ppp0",
            ]
        for ifname in interfaces:
            try:
                ip = get_interface_ip(ifname)
                break
            except IOError:
                pass
    return ip

