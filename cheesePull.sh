#!/bin/sh




if [ -f "/data/gfsbin/neededFiles.txt" ]
then
	file="/data/gfsbin/neededFiles.txt"
	while IFS= read -r line
	do
		
		
		scp -r pi@10.10.100.144:$line /data/gfsbin
		if [ "$line" ]
		then
			if [ -f $line ]
			then
				echo "===>Transfer of $line was successful.\n"
				chmod +x $line
			else
				echo "===>Transfer of $line was not successful.\n"
			fi
		else
			echo "===>ERR: Argument not given.\n"
		fi

	done <"$file"

	rm neededFiles.txt
else 
	echo "No files needed for transfer"
fi

