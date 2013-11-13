# USED TO TEST THE DATABASE FUNCTIONALITY

# These series of tests are used to make sure that the functionality
# of the database acts as it should, and does the appropriate data manipulation
# to the database when given a command. While not a dynamic test, it can be run
# if the database changes and catch any errors that may show up, assuming the 
# protocol remained the same. This test suite tests functionality for:
# INITIALIZE, FILE CREATE, CHUNK CREATE, DUPLICATE FILE CREATE, DELETION
# UNDELETION, LATEST CHUNKS, CHUNK LOCATIONS, and FILE DELETIONS


###############################################################

#	NOTE THAT THIS MUST BE RUN IN THE SAME FOLDER CONTAINING 
# 	database.py AND functionLibrary.py !!!!!!!!!!!!!!!!!!!!!

###############################################################


import database



def checkFlags(flag):
	if flag == -1:
		print "ERROR: -1"

	elif flag == -2:
		print "ERROR: -2"

	elif flag == -3:
		print "ERROR: -3"







# Initialize database!
database = database.Database()



# Check to see if the database initialized properly
print "\n"
print "--------------------------------------------------------------"
print "CHECKING: DATABASE INITIALIZE"
print "--------------------------------------------------------------"
print "data = ", database.data
print "lookup = ", database.lookup
print "toDelete = ", database.toDelete
print "chunkHandle = ", database.chunkHandle
print "\n"




# Now, lets add a file called testFile!

chunkHandle = database.getChunkHandle()

createFileFlag = database.createNewFile("testFile", chunkHandle)

# If the return flag was an error flag alert the error
checkFlags(createFileFlag)




# Now we can check to make sure it got created
print "\n"
print "--------------------------------------------------------------"
print "CHECKING: FILE CREATE"
print "--------------------------------------------------------------"
print "data = ", database.data
print "lookup = ", database.lookup
print "toDelete = ", database.toDelete
print "chunkHandle = ", database.chunkHandle
print "\n"

# and check that a chunk exists in the file with the correct chunkHandle
print "DOES THE CHUNK EXIST IN THE FILE?"
print "-------------------------------------------------"
print database.data["testFile"].chunks
print "\n"

# Now lets try to create a file of the same name!

print "--------------------------------------------------------------"
print "CHECKING: DUPLICATE FILE CREATE"
print "--------------------------------------------------------------"

chunkHandle = database.getChunkHandle()

createFileFlag = database.createNewFile("testFile", chunkHandle)

# If the return flag was an error flag alert the error
checkFlags(createFileFlag)

# Now we can check to make sure nothing changed
print "\n"
print "data = ", database.data
print "lookup = ", database.lookup
print "toDelete = ", database.toDelete
print "chunkHandle = ", database.chunkHandle
print "\n"



# Now lets try to create a file of different name!

print "--------------------------------------------------------------"
print "CHECKING: DIFFERENT FILE CREATE"
print "--------------------------------------------------------------"

chunkHandle = database.getChunkHandle()

createFileFlag = database.createNewFile("differentFile", chunkHandle)

# If the return flag was an error flag alert the error
checkFlags(createFileFlag)

# Now we can check to make sure a new file was created
print "\n"
print "data = ", database.data
print "lookup = ", database.lookup
print "toDelete = ", database.toDelete
print "chunkHandle = ", database.chunkHandle
print "\n"

# and check that a chunk exists in the file with the correct chunkHandle
print "DOES THE CHUNK EXIST IN THE FILE?"
print "-------------------------------------------------"
print database.data["differentFile"].chunks
print "\n"



# Lets mark the new file for deletion

print "--------------------------------------------------------------"
print "CHECKING: MARK FILE FOR DELETION"
print "--------------------------------------------------------------"

database.flagDelete("differentFile")


print "\n"
print "data = ", database.data
print "lookup = ", database.lookup
print "toDelete = ", database.toDelete
print "chunkHandle = ", database.chunkHandle
print "\n"


print "IS THE FILE MARKED FOR DELETE?"
print "-------------------------------------------------"
print "delete flag for differentFile = ", database.data["differentFile"].delete
print "\n"

# Lets unmark the new file for deletion

print "--------------------------------------------------------------"
print "CHECKING: MARK FILE FOR UNDELETION"
print "--------------------------------------------------------------"

database.flagUndelete("differentFile")


print "\n"
print "data = ", database.data
print "lookup = ", database.lookup
print "toDelete = ", database.toDelete
print "chunkHandle = ", database.chunkHandle
print "\n"

print "IS THE FILE MARKED FOR DELETE?"
print "-------------------------------------------------"
print "delete flag for differentFile = ", database.data["differentFile"].delete
print "\n"



# Create a new chunk for testFile

print "--------------------------------------------------------------"
print "CHECKING: CREATE NEW CHUNK"
print "--------------------------------------------------------------"


createChunkFlag = database.createNewChunk("testFile", "0", database.getChunkHandle())

checkFlags(createChunkFlag)


print "\n"
print "data = ", database.data
print "lookup = ", database.lookup
print "toDelete = ", database.toDelete
print "chunkHandle = ", database.chunkHandle
print "\n"


print "DOES THE NEW CHUNK EXIST?"
print "-------------------------------------------------"
print database.data["testFile"].chunks
print "\n"


# Getting the latest chunk from a file

print "--------------------------------------------------------------"
print "CHECKING: FILE'S LATEST CHUNK"
print "--------------------------------------------------------------"


print "\n"
print "data = ", database.data
print "lookup = ", database.lookup
print "toDelete = ", database.toDelete
print "chunkHandle = ", database.chunkHandle
print "\n"


print "WHAT IS THE LATEST CHUNK IN testFile"
print "-------------------------------------------------"
print database.findLatestChunk("testFile")
print "\n"



# Find the locations to a chunk

print "--------------------------------------------------------------"
print "CHECKING: LOCATIONS OF A CHUNK"
print "--------------------------------------------------------------"

print "Locations are added upon chunk creation from the functionLibrary chooseHosts(), which looks at activehosts.txt"

print "\n"
print "data = ", database.data
print "lookup = ", database.lookup
print "toDelete = ", database.toDelete
print "chunkHandle = ", database.chunkHandle
print "\n"


print "WHAT ARE THE LOCATIONS OF CHUNK 0 IN testFile?"
print "-------------------------------------------------"
print database.getChunkLocations("0")
print "\n"


# Delete a file!

print "--------------------------------------------------------------"
print "CHECKING: DELETE A FILE"
print "--------------------------------------------------------------"


database.flagDelete("testFile")

database.sanitizeFile("testFile")

print "\n"
print "data = ", database.data
print "lookup = ", database.lookup
print "toDelete = ", database.toDelete
print "chunkHandle = ", database.chunkHandle
print "\n"





print "--------------------------------------------------------------"
print "CHECKING: DELETE A FILE THAT DOES NOT EXIST"
print "--------------------------------------------------------------"

# We just deleted this file, so lets see what happens if it gets deleted again.
database.sanitizeFile("testFile")

print "\n"
print "data = ", database.data
print "lookup = ", database.lookup
print "toDelete = ", database.toDelete
print "chunkHandle = ", database.chunkHandle
print "\n"




print "--------------------------------------------------------------"
print "CHECKING: MAKE SURE getChunkHandle() WORKS"
print "--------------------------------------------------------------"


print "Let's see about getting 10 chunkhandles with database.getChunkHandle()"

for x in range(10):
	chunkHandle = database.getChunkHandle()
	print x, " : ", chunkHandle




