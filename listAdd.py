#
#			listAdd.py : an API for the entire cheeseWhiz family
#			By Torrent Glenn
#			11/13/2013
#
############################################################################################
""" listAdd.py, located on backus in /data/listAdd, is the new interface for 
managing what processes you want running and which you want dead. listAdd.py
takes input with either a -r (run)or a -k (kill) option followed by a list 
of processes. listAdd then resolves the intersection between hitlist.txt and
whizzifest.txt by removing the shared processes from the file which the user
was not adding to. i.e. if jankprocess.py is in whizzifest.txt running 
"python listAdd.py -k jankprocess.py" will add jankprocess to the hitlist
and remove it from the whizzifest. Hitman.py will kill processes on the 
hitlist while readwhizzifest.py will run processes in the whizzifest. 
As with any change to a file on backus, you MUST run mgen.py in order
for the pis to know what's happening. The -p (manual path entry option)
allows you to add entire paths i.e. "/foo/bar/process.py" as opposed to
the default "/data/gfsbin/" being added to the processes provided."""



import os
import sys 
import whizconfig

#remove the process name from the list for easier parsing
del sys.argv[0]
#create an empty string options in order to pick up any options
# i.e. -k, -r
options = ""

def getList(listToGet):
	#getList() parses a text file i.e. whizzifest or hitlist
	# and then appends the contents to a list
	newList = []
	with open(whizconfig.path+listToGet,"r") as whizFile:
		for line in whizFile:
			newList.append(line.strip("\n"))
		return newList

for item in sys.argv:
	#here we parse the command line input and scoop up 
	#anything that looks like an option (begins with "-")
	if item[0] == "-":
		#remove the items 
		sys.argv.remove(item)
		#and add them to the options string
		options += item

# in case some bozo tries to run and kill at the same time... we exit
if "r" in options and "k" in options:
	print "You need to choose whether to 'run' or 'kill'."
	exit(0)

#I had a simpler way to do this: with just "process = whizconfig.path + process"
#but i didn't work so I did this complicated reference to index thing to add the 
#appropriate path to each process
# the default is to add the path "/data/gfsbin/" to a process
# however, the option "-p" allows you to supply entire paths for the processes manually
if "p" not in options:
	for process in sys.argv:
		sys.argv[sys.argv.index(process)] = whizconfig.path + sys.argv[sys.argv.index(process)]

# create and populate the toRun and toKill lists with the contents of the respective files
toRun = []
toKill = []

if "m" in options:
	
	toRun += getList("whizzifestM.txt")
	toKill += getList("hitlistM.txt")
else:
	toRun += getList("whizzifestC.txt")
	toKill += getList("hitlistC.txt")	
# if we have the "run" option selected 
# add it (assuming it's not already there) to toRun
#remove any instance of it from toKill
if "r" in options:
	for process in sys.argv:
		if process in toKill:
			toKill.remove(process)
		if process not in toRun:
			toRun.append(process)
			
# if we have the "kill" option selected 
# add it (assuming it's not already there) to toKill
#remove any instance of it from toRun
elif "k" in options:
	for process in sys.argv:
		if process in toRun:
			toRun.remove(process)
		if process not in toKill:
			toKill.append(process)
			
#now overwrite the previous text files with the new list.
with open(whizconfig.path+"whizzifest.txt", "w") as whiz:
	for item in toRun:
		whiz.write(item+"\n")

with open(whizconfig.path+"hitlist.txt","w") as hit:
	for item in toKill:
		hit.write(item+"\n")



