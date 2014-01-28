#!/usr/bin/env python

import socket
import functionLibrary as fL

TCP_ADDRESS = '10.10.117.104'
TCP_PORT1 = 9666
TCP_PORT2 = 9666

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((TCP_ADDRESS, TCP_PORT1))

s.listen(1)

while 1:
	conn, addr = s.accept()
	data = fL.recv(conn)
	print data
	splitdata = data.split("|")
	#s.connect((API_ADDRESS, TCP_PORT2))
	if splitdata[0] == "CREATE":
		fL.send(conn, "10.10.117.104|01")
	elif splitdata[0] == "APPEND":
		fL.send(conn, "10.10.117.104|01")
	elif splitdata[0] == "READ":
		fL.send(conn, "READ|10.10.117.104*01*0")
	elif (splitdata[1] == 01):
		fL.send(conn, "poop")
	elif splitdata[0] == "CHUNKSPACE?":
		fL.send(conn, "10")
	elif splitdata[0] == "CREATECHUNK":
		fL.send(conn, "10.10.117.104|01")
	elif splitdata[0] == "DELETE":
		fL.send(conn, "MARKED")
	elif splitdata[0] == "UNDELETE":
		fL.send(conn, "MARKED")
	
