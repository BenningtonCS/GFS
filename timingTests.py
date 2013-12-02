import API, time, scrubber, socket, config, os
import functionLibrary as fL

# Initiate an instance of the API object
api = API.API()


itr = 10

logName = "Test10.txt"

fileName = "timingTest"

appendData = "Test"

errorList = []


for x in range(10):

	###################
	####### TEST CREATE
	###################

	# Starting time:
	createStart = time.time()

	# Test file/chunk creation. Create 10 new files consecutively.
	for x in range(itr):
		try:
			api.create(fileName + str(x))
		except:
			errorList.append('CREATE - Unable to create file&chunk: ' + fileName + str(x))

	# Ending time:
	createEnd = time.time()

	# Elapsed time:
	eTime = createEnd - createStart


	with open('create'+logName, "a") as timeFile:
		timeFile.write(str(eTime) + "\n")



	###################
	####### TEST APPEND
	###################

	appendStart = time.time()

	for x in range(itr):
		try:
			api.append(fileName + str(x), appendData, 0)
		except:
			errorList.append("APPEND FAILED: " + fileName + str(x))

	appendEnd = time.time()

	eTime = appendEnd - appendStart

	with open('append'+logName, 'a') as timeFile:
		timeFile.write(str(eTime) + '\n')


	#################
	####### TEST READ
	#################

	readStart = time.time()


	for x in range(itr):
		try:
			api.read(fileName + str(x), 0, -1, "test")
		except:
			errorList.append("READ FAILED: " + fileName + str(x))

	with open("test", "r") as testFile:
		data = testFile.read()

	print "READ DATA : ", data
	print "APPENDED DATA : ", appendData*itr

	if data != appendData*itr:
		errorList.append("READ DATA DOES NOT MATCH APPENDED DATA")

	readEnd = time.time()


	eTime = readEnd - readStart

	os.remove('test')

	with open('read'+logName, 'a') as timeFile:
		timeFile.write(str(eTime) + '\n')


	###################
	####### TEST DELETE
	###################

	deleteStart = time.time()

	for x in range(itr):
		# Try deleting a file!
		try:
			delflag = api.delete(fileName + str(x))

		except:
			errorList.append('DELETE - Mark deletion failure: ' + fileName + str(x))

		if delflag == -1:
			errorList.append('DELETE - Unable to mark for deletion: ' + fileName + str(x))
		elif delflag == -2:
			errorList.append('DELETE - The given file name does not exist: ' + fileName + str(x))
		elif delflag == -3:
			errorList.append('DELETE - The file has already been marked for deletion: ' + fileName + str(x))


	deleteEnd = time.time()

	eTime = deleteEnd - deleteStart


	with open('delete'+logName, 'a') as timeFile:
		timeFile.write(str(eTime) + "\n")



	print "initiate scrubber"
	#########################
	####### INITIATE SCRUBBER
	#########################

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
	print "connected"

	fL.send(s, "GETDELETEDATA|*")

	data = fL.recv(s)
	
	s.close()
	print "recvd to delete list"
	toDelete = []

	for item in data.split("|"):
		if item != "":
			toDelete.append(item)

	# Create an instance of the Scrubber object, and initiate it.
	scrub = scrubber.Scrubber(toDelete)

	scrubStart = time.time()

	scrub.clean()

	scrubEnd = time.time()

	eTime = scrubEnd - scrubStart

	with open('scrub'+logName, 'a') as timeFile:
		timeFile.write(str(eTime) + '\n')





	# If the error list has any errors in it, send an email to the specified
	# addresses containing the errors
	if errorList != []:
		# Import the email library
		import smtplib

		# Define the sender and recipients.
		# DO NOT CHANGE fromaddr PLEASE
		fromaddr = 'gfse2eerror@gmail.com'
		toaddrs  = ['edaniszewski@gmail.com']

		print errorList



'''
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
'''



