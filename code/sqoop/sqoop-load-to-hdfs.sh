# Ex 1: Transfer data from MySQL into HDFS

#=== Hostname = MySQL IP address or DNS name. Add your hostname here. 
#=== 3306 is the default MySQL Port.
#==== test is the database created in the earlier steps
#=== HDFS target directory is /user/cloudera/widgets 

#=== SSH into instance 1 and run the following script in the unix prompt
#=== Alternatively, you can also chmod+x sqoop-load-to-hdfs.sh 

# There will NOT be a corresponding table in Hive. The data would be stored under /user/cloudera/widgets. Verify the same. 
sudo -u hdfs
sqoop import   --connect "jdbc:mysql://localhost:3306/test" --username root  --password root  --table widgets --target-dir hdfs:/user/cloudera/widgets/  



# The target directory is optional. If dropped, the contents will be created under /user/<hdfs>/widgets directory
sqoop import   --connect "jdbc:mysql://localhost:3306/test" --username root  --password root  --table widgets  

# You can also specify a default parent warehouse directory  as follows. This is useful as a staging location across multiple sessions
sqoop import   --connect "jdbc:mysql://localhost:3306/test" --username root  --password root  --table widgets --warehouse-dir /user/etl/input/
 