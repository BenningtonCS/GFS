
#!/usr/bin/env python

import API
import time
from API import API

API = API()

while 1:
	operation = raw_input("What operation would you like to perform? : ")

	if (operation == "create"):
		filename = raw_input("What would you like the name of your file to be? : ")
		API.create(filename)

	elif (operation == "append"):
		filename = raw_input("What is the name of the target file? : ")
		newData = raw_input("What data would you like to add to the file? : ")
		API.append(filename, newData)

	elif (operation == "read"):
		filename = raw_input("What is the name of the target file? : ")
		byteOffSet = raw_input("What is your desired byte offset? : ")
		bytesToRead = raw_input("How many bytes would you like to read? : ")
		API.read(filename, byteOffSet, bytesToRead)
	
	elif (operation == "delete"):
		filename = raw_input("What is the name of the target file? : ")
		API.delete(filename)

	elif (operation == "undelete"):
		filename = raw_input("What is the name of the target file? : ")
		API.undelete(filename)
	
	elif (operation == "nothing"):
		break

	elif (operation != "create" or "append" or "read" or "delete" or "undelete" or "nothing"):
		print "That is an invalid operation. Please try again."
		continue

	time.sleep(2)
	#again = raw_input("Would you like to perform another operation? : ")	

	#if again == "no":
	#	break
	#elif again == "yes":
	#	continue
	#elif (again != "yes" or "no"):
	#	print "Please enter 'yes' or 'no' the next time you are asked if you'd like to continue."
	print "If you wish to no longer continue operations, when prompted for operation, input 'nothing'. "


