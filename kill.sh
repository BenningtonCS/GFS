#!/bin/sh

#for every argument which is passed to the shell script, kill it
#uage: sh kill.sh processid1 processid2 processid3 and so on
for var in $@
do
	kill $var
done

