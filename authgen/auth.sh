#!/bin/sh

# Run Script when first setting up/adding RPI server to grant key authorization (Password-less login) to master

#checks to see if .ssh directory exists
if [ -d ~/.ssh ]; then
	#if it exists, check to see if the id_rsa.pub file exists
        if [ -f ~/.ssh/id_rsa.pub ]; then
        	#if it exists, append it to the master server's authorized key list
                cat ~/.ssh/id_rsa.pub >> pi@10.10.100.144:~/.ssh/authorized_keys
                echo "id_rsa.pub already exists. Added to list of authorized_keys"
        else
        	#if the id_rsa.pub file does not exist, copy an authorized key (hosted on the server) to the new .ssh directory
                scp -r pi@10.10.100.144:~/.ssh/id_rsa ~/.ssh
                echo "Valid key established"
        fi
else
	#if the .ssh directory does not exist, create it and copy an authorized key (hosted on the server) to it.
        mkdir .ssh
        echo "Directory Made"
        scp -r pi@10.10.100.144:~/.ssh/id_rsa ~/.ssh
        echo "Valid key established"
fi

