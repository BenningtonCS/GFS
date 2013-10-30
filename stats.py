#!/usr/bin/env python

# import libraries
import config
import APIvers1
from APIvers1 import API

# stat page generator object
class statGen:
	def __init__(self):
		# body of the html file
		self.data = ''
		try:
			# open stats.html in the www folder
			self.file = open('/var/www/stats.html', 'w')
			# write initial html code
			self.file.write('<!DOCTYPE html><html><head><title>GFS Stats</title><link rel="stylesheet" type="text/css" href="style.css"></script></head><body>')
			self.file.write('<h1>DAT STATZ</h1>')
		# if file operations file, raise an exception and exit
		except:
			print "Couldn't generate stat file!"		
			exit()
	# makes a list of hosts and sees which ones are online
	def getHosts(self):
		try: 
			# open hosts file
			with open(config.hostsfile, 'r') as self.hostsFile:
				# make list of all hosts
				hosts = self.hostsFile.read().splitlines()
			# open active hosts file
			with open(config.activehostsfile, 'r') as self.activeHostsFile:
				# make a list of active hosts
				activeHosts = self.activeHostsFile.read().splitlines()
				# string to be appended to body
				string = '<h2>CHUNKSERVAZ</h2>'
				# from the list of all hosts, check which ones are
				# also in the active hosts file
				for x in range(0, len(hosts)):
					# if active, add OFFLINE to the end of string
					if hosts[x] not in activeHosts:
						string += hosts[x] + ' | <span class="offline">OFFLINE</span><br>'
					# if online, add ONLINE to end of string
					elif hosts[x] in activeHosts:
						string += hosts[x] + ' | <span class="online">ONLINE</span><br>'
				# append all data to the html body string			
				self.data += string
		# if hosts or active hosts file dont open, add an error in the HTML
		# body
		except:
			self.data += "Couldn't generate stats file!"		
	# get a list of files on the system
	def getFiles(self):
		try:
			# call the API
			self.API = API()
			# call the fileList method
			fileString = self.API.fileList()
			fileList = fileString.split("@")
			if(fileList[-1] == ""):
				fileList.remove("")
			fileDict = dict()
			for file in fileList:
				fileSplit = file.split("|")
				if fileSplit[0] in fileDict:
					fileDict[fileSplit[0]].append(fileSplit[1])
				else:
					fileDict[fileSplit[0]] = [fileSplit[1]]		
			self.data += "<h2>FILEZ</h2>"
			self.data += str(fileDict)
		except Exception as e:
			print e
			self.data += "Couldnt generate file list!"
	def getCat(self):
		self.data += '<h2>CAT</h2><img src="http://i.imgur.com/MRkP4yJ.jpg" />'
	def close(self):
		# append the html body to the stat file
		self.file.write(self.data)
		# append the closing html tags to the stat file
		self.file.write('</body></html>')
		# close stat file
		self.file.close()


##############################################################
statGen = statGen()
statGen.getHosts()
statGen.getFiles()
statGen.getCat()
statGen.close()
