sqlline.bat -u "jdbc:drill:zk=local"

!set maxwidth 10000

create view yelp_bus_view as select * from dfs.`D:/KB/Hadoop - Training/Hadoop Materials/data/yelp/yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json` ;


create table alias yelp_bus 
select * from dfs.'D:/KB/Hadoop - Training/Hadoop Materials/data/yelp/yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json'    limit 1;


select sum(review_count) as totalreviews from dfs.`D:/KB/Hadoop - Training/Hadoop Materials/data/yelp/yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json`;


select sum(review_count) as totalreviews from yelp_bus_view group by state, city order by count(*) desc limit 10;


//Top 10 cities by review_count
select state, city, count(*) totalreviews from dfs.`D:/KB/Hadoop - Training/Hadoop Materials/data/yelp/yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json`   group by state, city order by count(*) desc limit 10;


select count(*) as TotalRestaurants from dfs.`D:/KB/Hadoop - Training/Hadoop Materials/data/yelp/yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json` where true=repeated_contains(categories,'Restaurants');

select latitude,longitude,city from dfs.`D:/KB/Hadoop - Training/Hadoop Materials/data/yelp/yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json`  where city='Phoenix';

select count(*) from dfs.`D:/KB/Hadoop - Training/Hadoop Materials/data/yelp/yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json`   where city='Karlsruhe';










//Now store it in a temp file as CSV by using CTAS

use dfs.tmp
alter session set `store.format`='csv';

create table yelp_bus8 as  select type as type,business_id as business_id,name as name, flatten(neighborhoods) as neightborhoods, city,state,latitude,longitude, stars from dfs.`D:/KB/Hadoop - Training/Hadoop Materials/data/yelp/yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json`;


//Create a workspace


 

