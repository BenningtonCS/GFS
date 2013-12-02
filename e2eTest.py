#!/usr/bin/env python

#################################################################################
#                                                                             
#               GFS Distributed File System Master		              
#________________________________________________________________________________
#                                                                          
# Authors:		Erick Daniszewski                                            
#				Klemente Gilbert-Espada
#
#
# Date:			16 November 2013
# File:			e2eTest.py
#                                                                      
# Summary:		This script acts as a load/consistency test for our GFS
#				implementation. It is designed to perform all standard
#				API calls continuously on the system to ensure proper
#				functionality. 
#                                                                               
#################################################################################


import API, time, scrubber, socket, config, os
import functionLibrary as fL

# Initiate an instance of the API object
api = API.API()


# A list that will contain all the errors, if there are any
errorList = []

# Define the amount of delay between operations (in seconds)
dly = 0.5
# Define how many iterations of each operation you would like
itr = 10

# Create a file name to be used by all tests
fileName = "test0"
# Define what string will be appended
toAppend = "plague"


####### TEST CREATE

# Test file/chunk creation. Create 10 new files consecutively.
for x in range(itr):
	try:
		api.create(fileName + str(x))
		time.sleep(dly)
	except:
		errorList.append('CREATE - Unable to create file&chunk: ' + fileName + str(x))


####### TEST APPEND AND READ

# Test appending to chunks and reading from chunks. Append & read to 10 chunks consecutively.
for x in range(itr):
	# Try appending to a chunk!
	try:
		api.append(fileName + str(x), toAppend, 0)
		time.sleep(dly)

	except:
		errorList.append('APPEND - Unable to append to file: ' + fileName + str(x))

	# Try reading from the chunk!
	try:
		api.read(fileName + str(x), 0, -1, "e2eread")
		time.sleep(dly)
	except:
		errorList.append('READ - Unable to read from file: ' + fileName + str(x))	

	with open("e2eread", 'r') as readFile:
		data = readFile.read()

	os.remove('e2eread')

	if data != toAppend:
		errorList.append('APPEND/READ - ' + fileName + str(x) + ': Message appended does not match message read.')


####### TEST DELETE

for x in range(itr):
	# Try deleting a file!
	try:
		delflag = api.delete(fileName + str(x))
		time.sleep(dly)

	except:
		errorList.append('DELETE - Mark deletion failure: ' + fileName + str(x))

	if delflag == -1:
		errorList.append('DELETE - Unable to mark for deletion: ' + fileName + str(x))
	elif delflag == -2:
		errorList.append('DELETE - The given file name does not exist: ' + fileName + str(x))
	elif delflag == -3:
		errorList.append('DELETE - The file has already been marked for deletion: ' + fileName + str(x))


####### TEST UNDELETE

for x in range(itr):
	# Try deleting a file!
	try:
		delflag = api.undelete(fileName + str(x))
		time.sleep(dly)

	except:
		errorList.append('UNDELETE - Mark undeletion failure: ' + fileName + str(x))

	if delflag == -1:
		errorList.append('UNDELETE - Unable to unmark for deletion: ' + fileName + str(x))
	elif delflag == -2:
		errorList.append('UNDELETE - File was not marked for deletion: ' + fileName + str(x))





####### NEED TO REMARK FOR DELETE SO THE SCRUBBER CAN DO ITS BUSINESS

for x in range(itr):
	# Try deleting a file!
	try:
		delflag = api.delete(fileName + str(x))
		time.sleep(dly)

	except:
		errorList.append('DELETE - Mark deletion failure: ' + fileName + str(x))

	if delflag == -1:
		errorList.append('DELETE - Unable to mark for deletion: ' + fileName + str(x))
	elif delflag == -2:
		errorList.append('DELETE - The given file name does not exist: ' + fileName + str(x))
	elif delflag == -3:
		errorList.append('DELETE - The file has already been marked for deletion: ' + fileName + str(x))





####### INITIATE SCRUBBER

#try:
# Create a TCP socket instance
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# Set the timeout of the socket
s.settimeout(3)
# Allow the socket to re-use address
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
# Connect to the chunkserver over the specified port
s.connect((config.masterip, config.port))
#except:
#	print "CONNECTION ERROR"
#	errorList.append('SCRUBBER - Unable to connect to master')


fL.send(s, "GETDELETEDATA|*")

data = fL.recv(s)

toDelete = []

for item in data.split("|"):
	if item != "":
		toDelete.append(item)

# Create an instance of the Scrubber object, and initiate it.
scrub = scrubber.Scrubber(toDelete)
scrub.clean()





# If the error list has any errors in it, send an email to the specified
# addresses containing the errors
if errorList != []:
	# Import the email library
	import smtplib

	# Define the sender and recipients.
	# DO NOT CHANGE fromaddr PLEASE
	fromaddr = 'gfse2eerror@gmail.com'
	toaddrs  = ['edaniszewski@gmail.com']
	
	# Parse the error list to convert it to a string
	msg = ''
	for item in errorList:
		msg += "\n" + item


	# Credentials (if needed)
	username = 'gfse2eError'
	password = '@uIs0-s!~<<24'

	# The actual mail send
	server = smtplib.SMTP('smtp.gmail.com:587')
	server.starttls()
	server.login(username,password)
	server.sendmail(fromaddr, toaddrs, msg)
	server.quit()



