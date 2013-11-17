#!/usr/bin/env python

# This is a simple script to install psutil on whatever computer needs it so you
# don't have to remember what it is that you need to install in order to get psutil
# working.

import os

print "Updating apt-get"
os.system("sudo apt-get update")

print "Installing gcc"
os.system("sudo apt-get install gcc")

print "Installing python-dev"
os.system("sudo apt-get install python-dev")

print "Installing psutil"
os.chdir('psutil-1.1.3/')
os.system("sudo python setup.py install")

print "Everything should be installed and working."
