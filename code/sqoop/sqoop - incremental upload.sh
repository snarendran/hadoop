# Step 1: Create ST_MORTGAGE table in MySQL 

# The DDL files for creating ST_MORTGAGE is in z:\...data\rdbms\source\data\st_mortgage_mysql.ddl
mysql -u root -p (and enter root as the password when prompted)
use sqoop;
\. st_mortgage.ddl.csv

# Step 2: Import sample data into ST_MORTGAGE in MySQL. 
# Assumes that the st_mortgage data file is in /opt/downloads/st_mortgage.csv
cp /opt/downloads/st_mortgage.csv /var/lib/mysql/sqoop
LOAD DATA INFILE 'st_mortgage.csv' INTO TABLE ST_MORTGAGE
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

 
# Step 3a: Initial LOAD from MySQL to Hive using Sqoop. This is a one time activity. After loading data into Hive, rename st_mortgage table to st_mortgage_base. 
# This will be the base directory. 
sqoop import \
--connect jdbc:mysql://localhost:3306/sqoop \
--username sqoop \
--password sqoop \
--table ST_MORTGAGE \
--hive-import \
--fields-terminated-by ',' \
--map-column-hive CHANGEDATE=timestamp \
--warehouse-dir /user/hive/warehouse -m 1

# Verify the same in Hive. Row count should be 200; Verify some sample records to ensure that the right data set is loaded.
Step 3b: Rename st_mortgage to st_mortgage_base
# Next, rename the base table ownership to cloudera so that you can manipulate through cloudera 
# hadoop fs -chown -R cloudera /user/hive
alter table st_mortgage rename to st_mortgage_base;

select count(*) from default.st_mortgage_base;


# Step 4: Define a Job for incremental Data. This is just the job definition  and is not run immediately. 
 sqoop job --delete inc_import;

sqoop job --create inc_import \
 -- \
 import --connect jdbc:mysql://localhost:3306/sqoop \
 --append \
 --username sqoop \
 --password sqoop \
 --table ST_MORTGAGE \
 --target-dir /user/hive/warehouse/st_mortgage_inc \
 --check-column CHANGEDATE \
 --incremental lastmodified \
 --last-value {lastmodified} -m 1;
   

# Step 5: Ingest data changes in MySQL
# loannumber column will be the key;
# CHANGEDATE,CHANGETIME will be used for loading incremental data
# select loannumber, CREDITSCORE from ST_MORTGAGE limit 1;
# loannumber=1000003267, CREDITSCORE=561

#Change the Credit Score and add populate the changedate field as well
update ST_MORTGAGE set CREDITSCORE=461, CHANGEDATE =CURRENT_TIMESTAMP where loannumber='1000038274'; 
commit;

# Test if the change has been applied
select loannumber,CHANGEDATE  from ST_MORTGAGE where CHANGEDATE > '0000-00-00 00:00:00';

# Step 6: Import incremental data into st_mortgage_inc in Hive by running the sqoop job 
# Create st_mortgage_inc as external table and point to /user/hive/warehouse/st_mortgage_inc. This table will hold the incremental values
create external table st_mortgage_inc like st_mortgage_base 
   LOCATION   
 # Enter sqoop as the password when prompted
 sqoop job   --exec inc_import
 # sudo hdfs; hadoop fs -chown -R cloudera /user/hive/warehouse/st_mortgage_inc

# Step 7: Reconcile View
CREATE VIEW st_mortgage_view AS
SELECT t1.* FROM
(SELECT * FROM st_mortgage_base
    UNION ALL
    SELECT * FROM st_mortgage_inc) t1
JOIN
    (SELECT loannumber, max(CHANGEDATE) max_modified FROM
        (SELECT * FROM st_mortgage_base
        UNION ALL
        SELECT * FROM st_mortgage_inc) t2 
    GROUP BY loannumber) s 
ON t1.loannumber = s.loannumber AND t1.CHANGEDATE = s.max_modified;


# Step 7: Reporting Table
DROP TABLE st_mortgage_rpt;
CREATE TABLE st_mortgage_rpt AS
SELECT * FROM st_mortgage_view;


# Step 8: Purge old Base table and copy the Report table; Need to run this before the next incremental load
DROP TABLE st_mortgage_base;
CREATE TABLE st_mortgage_base AS
SELECT * FROM st_mortgage_rpt;

hadoop fs –rm –r /user/hive/st_mortgage_inc/*

# validate the change 
select loannumber, creditscore, changedate from st_mortgage_base where loannumber='1000038274';


# Repeat the change capture;
update ST_MORTGAGE set CREDITSCORE=6666, CHANGEDATE =CURRENT_TIMESTAMP where loannumber='1000003267'; 
insert into ST_MORTGAGE( CREDITSCORE,CHANGEDATE,loannumber)values (6666, CURRENT_TIMESTAMP, '1000003267'); 
CURRENT_TIMESTAMP 

# cannot handle delete 
delete from ST_MORTGAGE where loannumber='1000003267'; 
sqoop job --exec inc_import

# Drop and recreate the report table; 
DROP TABLE st_mortgage_rpt;
CREATE TABLE st_mortgage_rpt AS
SELECT * FROM st_mortgage_view;


# Drop and recreate the base table;
DROP TABLE st_mortgage_base;
CREATE TABLE st_mortgage_base AS
SELECT * FROM st_mortgage_rpt;