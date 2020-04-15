#!/bin/sh
file="/home/cloudera/git/BDT-Hotel/input/hotel_bookings.csv"
count=1
delay=1

while IFS= read line
do
        
	if [ $count -ne 1 ]; then
		echo "$line"
		modulo=$(( $count  % 111 ))
		if [ $modulo -eq 0 ]; then
   			sleep $delay
		fi
	fi
	count=$[$count+1]
	
done <"$file"

