#!/bin/sh

jarFileLocation='/home/cloudera/git/BDT-Hotel/target'
jarFile='BDT_Hotel-0.0.1-SNAPSHOT-jar-with-dependencies.jar'
mainClass='cs523.Final.SparkSQL'
additionalLibs='/home/cloudera/git/BDT-Hotel/libs/*'
delay=300;

cd $jarFileLocation

while true
do
	
	java -cp "$jarFile:$additionalLibs" $mainClass
	sleep $delay
	
done