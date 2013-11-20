#!/usr/bin/env python

import API,sys
from API import API

API = API()


if (sys.argv[1] == "files"):
        API.fileList()
              

if sys.argv[1] == "create":
	filename = sys.argv[2] 
	API.create(filename)

elif sys.argv[1] == "append":
	filename = sys.argv[2] 
	newData = sys.argv[3] 
	API.append(filename, newData, True)

elif sys.argv[1] == "read":
	filename = sys.argv[2] 
	byteOffSet = sys.argv[3]
	bytesToRead = sys.argv[4]
	newName = sys.argv[5]
	API.read(filename, byteOffSet, bytesToRead, newName)

elif sys.argv[1] == "delete":
	filename = sys.argv[2] 
	API.delete(filename)

elif sys.argv[1] == "undelete":
	filename = sys.argv[2] 
	API.undelete(filename)
	
elif (sys.argv[1] == "files"):
	filename = sys.argv[2] 
	API.fileList()
        
       




