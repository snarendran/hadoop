# Ex 2: Transfer data from MySQL into HDFS and create metastore in Hive

#=== Hostname = MySQL IP address or DNS name. Add your hostname here. 
#=== 3306 is the default MySQL Port.
#==== test is the database created in the earlier steps
#=== HDFS target directory is /user/cloudera/widgets 

#=== SSH into instance 1 and run the following script in the unix prompt
#=== Alternatively, you can also chmod+x sqoop-load-to-hdfs.sh 
 

sudo -u hdfs
sqoop import   --connect "jdbc:mysql://localhost:3306/test" --username root  --password root  --table widgets --target-dir hdfs:/user/cloudera/widgets/  --m 1

-- There will NOT be a corresponding table in Hive. The data would be stored under /user/cloudera/widgets/ directory. Verify the same. 