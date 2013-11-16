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


import API, time
import functionLibrary as fL


# Initiate an instance of the API object
api = API.API()


# Test file/chunk creation. Create 10 new files consecutively.
for x in range(10):
	api.create('testfile' + str(x))
	time.sleep(.5)

# Test appending to chunks. Append to 10 chunks consecutively.
for x in range(10):
	api.append('testfile' + str(x), "")
	time.sleep(0.5)

# Test reading from chunks. Read from 10 chunks consecutively. 
for x in range(10):
	api.read('testfile' + str(x), 0, -1)
	time.sleep(0.5)