#  
 

sudo -u hdfs

# open up mysql prompt and enter create another table called country

CREATE TABLE countries
(
	isd_code int,
	name  varchar(255)
);

insert into countries (1, 'USA'), (2,'Germany'), (3, 'India');

# Import all the tables, but for countries. Technically, this will import just the cities table alone

sqoop import-all-tables \
	--connect jdbc:mysql://localhost:3306/sqoop \
	--username sqoop \
	--password sqoop \
	-- exclude-tables countries
