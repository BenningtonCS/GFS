#!/usr/bin/env python

import config

class statGen:
	def __init__(self):
		self.data = ''
		try:
			self.file = open('/var/www/stats.html', 'w')
			self.file.write('<!DOCTYPE html><html><head><title>Page Title</title><link rel="stylesheet" type="text/css" href="style.css"></script></head><body>')
		except:
			print "Couldn't generate stat file!"		
	def getHosts(self):
		try: 
			with open(config.activehostsfile, 'r') as self.activeHostsFile:
				hosts = self.activeHostsFile.read().splitlines()
				string = '<h1>Active Hosts</h1>'
				for x in range(0, len(hosts)):
					string += hosts[x] + "<br>"
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
