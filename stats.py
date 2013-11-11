#!/usr/bin/env python

# import libraries
import config, APIvers1, sys, logging, urllib2
from datetime import datetime
from datetime import timedelta
from APIvers1 import API

# logging
args = sys.argv
# Check to see if the verbose flag was one of the command line arguments
if "-v" in args:
        # If it was one of the arguments, set the logging level to debug 
        logging.basicConfig(level=logging.DEBUG, format='%(levelname)s : %(message)s')
else:
        # If it was not, set the logging level to default (only shows messages with level
        # warning or higher)
        logging.basicConfig(filename='masterLog.log', format='%(asctime)s %(levelname)s : %(message)s')


# stat page generator object
class statGen:
	def __init__(self):
		# body of the html file
		self.data = ''
		try:
			# open stats.html in the www folder
			self.file = open('/var/www/pages/stats.php', 'w')
		# if file operations file, raise an exception and exit
		except:
			logging.error("Couldn't generate stat file!")		
			exit()
	def getMasterStats(self):
		string = ''
		string += "<h2>Master Statistics</h2>"
                string += "<p><strong>IP Address:</strong> " + config.masterip + "</p>"
		try:
			f = open('/proc/uptime', 'r')
			uptime_seconds = float(f.readline().split()[0])
			uptime_string = str(timedelta(seconds = uptime_seconds))
			string += "<p><strong>Uptime: </strong>" + uptime_string + "</p>"
		except Exception as e:
			string += "<p>" + str(e) +"</p>"
		self.data += string		
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
				string = '<h2>Chunkservers</h2>'
				string += '<div class="table-responsive"><table class="table table-hover"><tr><th>Server IP</th><th>Status</th><th>Space Usage</th><th>View Logs</th>'
				# from the list of all hosts, check which ones are
				# also in the active hosts file
				for x in range(0, len(hosts)):
					string += '<tr>'
					# if active, add OFFLINE to the end of string
					if hosts[x] not in activeHosts:
						string += '<td>' + hosts[x] + '</td><td><span class="label label-warning">OFFLINE</span></td>'
					# if online, add ONLINE to end of string
					elif hosts[x] in activeHosts:
						string += '<td>' + hosts[x] + '</td><td><span class="label label-success">ONLINE</span></td>'
					try:
						file = urllib2.urlopen("http://"+ hosts[x] +":8000/httpServerFiles/stats.txt")
						fileData = file.read().split('|')
						difference = str(float(fileData[1]) - float(fileData[0]))
					except IOError:
						logging.error("Couldnt read stats file")
					string += '<td><progress max="'+fileData[1]+'" value="'+ difference +'"></progress> '+fileData[0]+'GB/'+fileData[1]+'GB</td><td><a href="http://' + hosts[x] +':8000/httpServerFiles/chunkserverLog.log">View Log</a></td></tr>'
				string += '</table></div>'
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
	def getLog(self):
		try:
			masterLogFile = open('masterLog.log', 'r')
			masterLog = masterLogFile.read()
			self.data += "<h2>Master Log</h2>"
			self.data += '<textarea rows="15" class="form-control">' + masterLog + "</textarea>"
		except:
			self.data += "Couldn't generate master log!"
	def getCat(self):
		self.data += '<h2>CAT</h2><img src="http://i.imgur.com/MRkP4yJ.jpg" />'
	def close(self):
		# append the html body to the stat file
		self.file.write(self.data)
		self.file.write("<p>Generated at: " + str(datetime.now()) + "</p>")
		# close stat file
		self.file.close()


##############################################################
statGen = statGen()
statGen.getMasterStats()
statGen.getHosts()
#statGen.getFiles()
statGen.getLog()
#statGen.getCat()
statGen.close()
