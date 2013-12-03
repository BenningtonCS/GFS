#!/bin/sh

#for every argument which is passed to the script, run it
#usage: sh run.sh /path/to/process1 /path/to/process2 /and/so/on
for var in $@
do
	$var &
done
exit 0
