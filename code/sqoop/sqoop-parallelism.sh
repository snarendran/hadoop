# Ex 6: Parallelism using num-mappers

 

# There will NOT be sa corresponding table in Hive. The data would be stored under /user/cloudera/widgets. Verify the same. 
sudo -u hdfs

sqoop import \
--connect jdbc:mysql://mysql.example.com/sqoop \
--username sqoop \
--password sqoop \
--table cities \
--num-mappers 10
