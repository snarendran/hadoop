# Ex 2: Transfer data from MySQL into HDFS and create metastore in Hive
# the --warehouse-dir switch is optional. 
# If not mentioned the table will be loaded under /user/<user_name>/cities ...in this case /user/hdfs/cities (as the script is run as hdfs user)
# The load is treated as an external table. 

sudo -u hdfs

sqoop import \
--connect jdbc:mysql://localhost:3306/sqoop \
--username sqoop \
--password sqoop \
--table cities \
--hive-import \
--warehouse-dir /user/hive/warehouse


 
 
-- Verify that a Hive  table got created under the default database
-- Verify also that an "external"  table got created under /user/hive/warehouse (Delete the cities table in Hive and you will still find the underlying data intact)