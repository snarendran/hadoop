# create staging_cities in MySQL

CREATE TABLE staging_cities
  AS (SELECT *
      FROM staging WHERE 1=2);


sqoop export \
--connect jdbc:mysql://mysql.example.com/sqoop \
--username sqoop \
--password sqoop \
--table cities \
--staging-table staging_cities


