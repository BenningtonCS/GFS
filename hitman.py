				 ######################################################
				# 	hitman.py 				     #
			       #       Torrent Glenn                 		    #
			      #        11/13/2013				   #
			     #	       CheeseWhiz revamped                        #
			    ######################################################
				
""" The hitman is exactly what the cheesewhiz family has been lacking all this time. Now the hitman
(essentially the opposite of cheesewhiz) simply kills all the processes you want dead. Another addition, listAdd.py 
is the new interface for managing the cheesewhiz family"""

import os
import functionLibrary as fL

def rmSpace(spaceList):
	#Later in the code when the information returned by ps-ef
	#needs to be parsed this function removed the empty strings
	#in the list in order to make it easier to retrieve 
	#information via indexes
	#rmSpace() recursively removes the first empty string in the 
	#given list until there are no more empty strings
	if '' in spaceList:
		spaceList.remove('')
		rmSpace(spaceList) 
	else: 
		return spaceList


#Open the hitlist, get all the lines, and put them in a list


thisMachineIp = fL.get_lan_ip()

with open("machineFunction.txt","r") as machineList:
	for line in machineList:
 		SplitLine = line.split("|")
 		if thisMachineIp == SplitLine[0]:
 			machineType =  SplitLine[1]
print machineType
if machineType == "C":
	with open('hitlistC.txt','r') as w:
        	content = w.readlines()
elif machineType == "M":
	with open('hitlistM.txt','r') as w:
		content = w.readlines()


# make a new list wherein all the newline characters have been stripped
programs = [elem.strip("\n") for elem in content]



# runs the unix script to output the results of ps-ef into ps.txt
#os.system("top -n 1 -b > top.txt")
os.system("ps -ef > psh.txt")

# create an empty list to be populated with the results of parsing ps.txt
tst = []

# from ps.txt, parse each line to get the name of the process and append it to tst
with open('psh.txt', 'r') as input:
        for line in input:
                newline = line.split(' ')
		rmSpace(newline)
                tst.append(newline[1].strip())

# create an empty string to be populated by the subset of all processes which are running 
#that we want to kill
running = ""

# get the PIDs of the programs to kill and append them to IDs.txt
for item in programs:
	os.system("ps -ef | grep "+ item +" >> IDs.txt")
	
# the list of programs we want dead
toKill = []

#here we put all the PIDs of programs we want to kill in the toKill list 
# and remove trailing whitespace
with open ("IDs.txt","r") as input:
	for line in input:
		newline = line.split(" ")
		rmSpace(newline)
		toKill.append(newline[1].strip())

#get the intersection of processes that are running and processes we want to kill
for item in toKill:
        if item in tst:
                running += " " + item


# kill the programs in the aforementioned intersection: programs we want to kill that
# are running
os.system("kill -9 " + running)
#remove IDs.txt
os.system("rm IDs.txt psh.txt")

