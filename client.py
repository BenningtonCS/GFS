#!/usr/bin/env python

import API,sys
from API import API

API = API()
	
filename = sys.argv[2] 

if sys.argv[1] == "create":
	API.create(filename)

elif sys.argv[1] == "append":
	newData = sys.argv[3] 
	API.append(filename, newData, True)

elif sys.argv[1] == "read":
	byteOffSet = sys.argv[3]
	bytesToRead = sys.argv[4]
	newName = sys.argv[5]
	API.read(filename, byteOffSet, bytesToRead, newName)

elif sys.argv[1] == "delete":
	API.delete(filename)

elif sys.argv[1] == "undelete":
	API.undelete(filename)




