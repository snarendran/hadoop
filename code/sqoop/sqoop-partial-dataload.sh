# Ex 2: Transfer data from MySQL into HDFS and create metastore in Hive

#=== Hostname = MySQL IP address or DNS name. Add your hostname here. 
#=== 3306 is the default MySQL Port.
#==== test is the database created in the earlier steps
#=== HDFS target directory is /user/cloudera/widgets 

#=== SSH into instance 1 and run the following script in the unix prompt
#=== Alternatively, you can also chmod+x sqoop-load-to-hdfs.sh 
# hadoop fs -rm /user/hive/warehouse/cities/*; hadoop fs -rm /user/hive/warehouse/*; hadoop fs -rmdir /user/hive/warehouse/cities;hadoop fs -rmdir /user/hive/warehouse


sudo -u hdfs
 

# To load specific columns use the syntax below. Here, we have dropped the city column. The targete-dir, split-by and $CONDITIONS are all mandatory

sqoop import \
--connect jdbc:mysql://localhost:3306/sqoop \
--username sqoop \
--password sqoop \
--query  'select id, country from cities  where    $CONDITIONS' \
--target-dir /user/hive/warehouse \
--split-by id 


# To load specific record set. Eg. to import all those records whose country = USA

sqoop import \
--connect jdbc:mysql://localhost:3306/sqoop \
--username sqoop \
--password sqoop \
--table cities \
--where "country = 'USA'"


 