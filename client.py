#!/usr/bin/env python

import APIvers1
import time
from APIvers1 import *

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
	
	time.sleep(3)
	again = raw_input("Would you like to perform another operation? : ")
	
	if again == "no":
		break
	elif again == "yes":
		pass
