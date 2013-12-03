#!/bin/sh

scp -r pi@10.10.100.144:"/data/gfsbin/manifest.txt" "/data/temp/manifest.txt"

echo "===> maniFetched!!!\n"