#!/usr/bin/env python

#################################################################
#								#
#	CHUNK SERVER EMULATOR FOR HEARTBEAT TESTING		#
#								#
#################################################################







#	!!!!REMEMBER!!!! 

#	Before you run this test, make sure that you update the hosts.txt
#	in the directory containing heartBeat.py so it knows who its talking to!
#
#	Also, make sure that the port this emulator is listening on is the same
#	port that heartBeat.py is communicating on!








import socket, time

ADDRESS = ''
PORT = 9666



def handleSleep():
        # Get user input
        delayTime = raw_input("How many seconds should the chunkserver wait before sending a response? ")
        try:
		# Make sure the user input is a valid type
                delayTime = int(delayTime)
		if delayTime < 0:
			print "tsk tsk, you can't have a negative delay! I'll make that positive for you"
			delayTime = delayTime * -1
		return delayTime
        # If the user input is not an integer, re-prompt the user
        except ValueError:
                print "\nERROR: Input value is not a number. Try again!"
                handleSleep()

def handleMessage():
        # Get user input
        message = raw_input("What message should the chunkserver send to heartBeat? Leave blank for default '<3!' :  ")
        # If left blank, set the message to <3!
        if message == "":
                message = '<3!'
	# Return the user-defined message
	return message



# get user-defined message send delay and message contents
delayTime = handleSleep()
message = handleMessage()

# initialize, allow address re-use, and bind to port
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((ADDRESS, PORT))
print "Initialized and bound"

# listen for the heartBeat message
s.listen(1)
print "Listening"

# accept the heartBeat message connection and receive the data
conn, addr = s.accept()
data = conn.recv(1024)
print "Received: ", data

# if the data was the heartBeat message, wait for the specified time, and send the specified message back
if data == "<3?":
	print "Wait for ", delayTime, " seconds"
	time.sleep(int(delayTime))
	conn.send(message)
	print "Sent message: ", message

# close the socket
s.close()
