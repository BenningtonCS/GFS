#!/bin/sh




if [ -f "/data/PackRat/neededFiles.txt" ]
then
	file="/data/PackRat/neededFiles.txt"
	while IFS= read -r line
	do
		
		
		scp -r pi@10.10.100.144:$line /data/PackRat
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

