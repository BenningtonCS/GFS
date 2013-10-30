#!/usr/bin/env python

import config

class statGen:
	def __init__(self):
		self.data = ''
		try:
			self.file = open('/var/www/stats.html', 'w')
			self.file.write('<!DOCTYPE html><html><head><title>GFS Stats</title><link rel="stylesheet" type="text/css" href="style.css"></script></head><body>')
			self.file.write('<h1>DAT STATZ</h1>')
		except:
			print "Couldn't generate stat file!"		
			exit()
	def getHosts(self):
		try: 
			with open(config.hostsfile, 'r') as self.hostsFile:
				hosts = self.hostsFile.read().splitlines()
			with open(config.activehostsfile, 'r') as self.activeHostsFile:
				activeHosts = self.activeHostsFile.read().splitlines()
				string = '<h2>Chunkservers</h2>'
				for x in range(0, len(hosts)):
					if hosts[x] not in activeHosts:
						string += hosts[x] + " | OFFLINE<br>"
					elif hosts[x] in activeHosts:
						string += hosts[x] + " | ONLINE<br>"
				self.data += string
		except:
			self.data += "Couldn't generate stats file!"		
	def close(self):
		self.file.write(self.data)
		self.file.write('</body></html>')
		self.file.close()


##############################################################
statGen = statGen()
statGen.getHosts()
statGen.close()
