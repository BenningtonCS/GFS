#!/usr/bin/env python

hostsfile = 'hosts.txt'
activehostsfile = 'activehosts.txt'
oplog = 'opLog.txt'
port = 9666
masterip = '10.10.100.144'
chunkPath = '/data/gfsbin/Chunks'
chunkSize = 12000 #bytes
debug = False

# EOT (End Of Transmission) is an ASCII character whose integer value on an ASCII table
# is 4. chr() is the function used to convert an integer to an ASCII symbol, so since the
# EOT character is untypable, it is created by converting from its integer value.
# This is the character to be used at the end of all sent messages so recv knows when to terminate
eot = chr(4)
