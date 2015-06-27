# Ex 2: Transfer data from MySQL into HDFS and create metastore in Hive
# the --warehouse-dir switch is optional. 
# If not mentioned the table will be loaded under /user/<user_name>/cities ...in this case /user/hdfs/cities (as the script is run as hdfs user)
# The load is treated as an external table. 
# hadoop fs -rm /user/hive/warehouse/cities/*; hadoop fs -rm /user/hive/warehouse/*; hadoop fs -rmdir /user/hive/warehouse/cities;hadoop fs -rmdir /user/hive/warehouse;hadoop fs -rmdir /user/hive/warehouse/cities

#hadoop fs -rm /user/hive/warehouse/cities/city="Sunnyvale"/*;  hadoop fs -rmdir /user/hive/warehouse/cities/city="Sunnyvale";  

# The data will be loaded under /user/hive/warehouse/cities/city=Ssunnyvale. However, since there is no filter in the --query clause, all the records will be loaded into Sunnyvale directory
sudo -u hdfs

sqoop import \
--connect jdbc:mysql://127.0.0.1:3306/sqoop \
--username sqoop \
--password sqoop \
--query  'select id,  country from cities  where city="Sunnyvale"  and  $CONDITIONS' \
--split-by id \
--hive-import \
--hive-table cities \
--target-dir /user/tmp/cities \
--hive-partition-key city \
--hive-partition-value "Sunnyvale";

# Load the rest of the tables into others 
sqoop import \
--connect jdbc:mysql://127.0.0.1:3306/sqoop \
--username sqoop \
--password sqoop \
--query  'select id,  country from cities  where city !="Sunnyvale" and  $CONDITIONS' \
--split-by id \
--hive-import \
--hive-table cities \
--target-dir /user/hive/tmp/cities \
--hive-partition-key city \
--hive-partition-value "Others"

# Is there a way to optimize the two into one batch script?





 