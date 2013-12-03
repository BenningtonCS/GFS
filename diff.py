#! /usr/bin/env python

import os
import PackRatConfig as PRconfig


def checkDiff(PL,SL):
		li = []
		for part in PL:
			i = 0
			for par in SL:
				if part[1] in par:
					i = 1
			if i == 0:
				li.append(part[0])
		return li

def main():

	piman = open(PRconfig.path+"/manifest.txt") 
	servman = open(PRconfig.path+"/temp/manifest.txt") 

	if piman.readline() == servman.readline():

		return 0
	else:
		newman = "go"

	piman.close()
	servman.close()

	piman = open(PRconfig.path+"/manifest.txt") 
	servman =  open(PRconfig.path+"/temp/manifest.txt") 


	piL = []
	svL = []
	PL = []
	SL = []
	ln = []
	RealPull = []
	
	for line in piman:
		piL.append(line)
	del piL[0]

	for line in servman:
		svL.append(line)
	del svL[0]

	for obj in piL:
		PL.append(obj.split("|"))

	for obj in svL:
		SL.append(obj.split("|"))

	toDelete = checkDiff(PL,SL)
	toPull = checkDiff(SL,PL)

	for thing in toDelete:
		if thing in toPull:
			toDelete.remove(thing)

	for c in toDelete:
		if os.path.exists(c):
			os.remove(c)
			print "===>Deleted ", c ,"\n"

	NewFi = open(PRconfig.path+"/neededFiles.txt", "w")


	for item in toPull:
                ln.append(item.split("/"))
        for item in ln:
                while len(item) > 4:
                        del item[4]
        for item in ln:
        	RealPull.append('/'.join(item))

	for item in RealPull:
		NewFi.write(item)
		NewFi.write("\n")
		
	NewFi.write(PRconfig.path+"/manifest.txt\n")
	 	
	piman.close()
	servman.close()
	NewFi.close()

	return 0

main()
