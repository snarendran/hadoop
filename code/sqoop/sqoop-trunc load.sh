sudo -u hdfs
 
sqoop import \
--connect jdbc:mysql://localhost:3306/sqoop \
--username sqoop \
--password sqoop \
--table cities \
--hive-overwrite \
--hive-import \
--warehouse-dir /user/hive/warehouse
