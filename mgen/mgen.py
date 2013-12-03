#! /usr/bin/env python

#########################################################################
#									#
#	mgen.py creates a new version of a manifest containing the	# 
#	contents of a specified directory. The manifest includes the	#
#	name of the file and the path of the file. It also creates 	#
#	an md5 checksum for each item in the manifest.			#
#									#
#########################################################################


import os
import hashlib
import PackRatConfig as PRconfig
# Define the directory path mgen.py will work in
GFS_PATH = PRconfig.path+"/"

# If the manifest file already exists in the defined path, get the version number from line 1 and store it
# in memory, then remove the manifest
if os.path.isfile(GFS_PATH + 'manifest.txt'):
        with open(GFS_PATH + 'manifest.txt', 'r') as f:
                VERSION_STR = f.readline()
		VERSION = int(VERSION_STR)
        os.remove(GFS_PATH + 'manifest.txt')

# Create a new manifest version
with open(GFS_PATH + "manifest.txt", "w") as a:
	# Increment the version number
        NEW_VERSION = str(VERSION + 1)
	# Write the version number on line 1 of the manifest
        a.write(NEW_VERSION + os.linesep)
	# Look at all files and subdirectories in the defined directory and add them to the manifest
        for path, subdirs, files in os.walk(GFS_PATH):
          
		
		
			
			for filename in files:
				
                	        f = os.path.join(path, filename)
				
				#print f
				# For all files that are not the manifest, create an md5 checksum
                       		if f != PRconfig.path+"/manifest.txt":
                        	        hex = hashlib.md5(open(str(f)).read()).hexdigest()
                               		a.write(str(f) + "|" + hex + os.linesep)

# Create an md5 checksum for the manifest itself
with open(GFS_PATH + "manifest.txt", "r") as a:
        mhex = hashlib.md5(a.read()).hexdigest()

# Append the manifest md5 checksum to the manifest
with open(GFS_PATH + "manifest.txt", "a") as a:
        a.write(GFS_PATH + "manifest.txt" + "|" + mhex + os.linesep)
