#!/bin/sh

#start hbase service if it automatically stopped
echo "---> start hbase-master and hbase-regionserver"
sudo service hbase-master start | sudo service hbase-regionserver start

#create table
hiveCreateTable="/home/cloudera/git/BDT-Hotel/script/Hive_CreateTable.sql"
#Validate table
hiveTableValidate="/home/cloudera/git/BDT-Hotel/script/Hive_Validate.sql"

echo "---> create and import Hive table from HBase"
hive -f $hiveCreateTable
echo "---> display imported Hive table"
hive -f $hiveTableValidate
