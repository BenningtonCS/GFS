#!/usr/bin/env python

import os, logging


disk = os.statvfs("/data")
capacity = disk.f_bsize * disk.f_blocks
available = disk.f_bsize * disk.f_bavail

try:
	file = open("httpServerFiles/stats.txt", "w")
	file.write(str(available/1.073741824e9) +"|" + str(capacity/1.073731824e9))
except IOError:
	logging.error("Couldn't make stats file")

