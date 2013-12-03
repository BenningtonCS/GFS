#!/bin/sh

SERVER='10.10.100.144'
USER=pi
./manifetch.py
./diff.py
./cheesePull.sh ${SERVER} ${USER}
