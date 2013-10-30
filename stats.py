#!/usr/bin/env python

# import libraries
import config

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
				string = '<h2>Chunkservers</h2>'
				# from the list of all hosts, check which ones are
				# also in the active hosts file
				for x in range(0, len(hosts)):
					# if active, add OFFLINE to the end of string
					if hosts[x] not in activeHosts:
						string += hosts[x] + " | OFFLINE<br>"
					# if online, add ONLINE to end of string
					elif hosts[x] in activeHosts:
						string += hosts[x] + " | ONLINE<br>"
				# append all data to the html body string			
				self.data += string
		# if hosts or active hosts file dont open, add an error in the HTML
		# body
		except:
			self.data += "Couldn't generate stats file!"		
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
statGen.close()
