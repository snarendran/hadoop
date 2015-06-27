-- Process the weblogs
-- Objective: how many weblogs correspond to jpg images (Note: Look for files with jpg extension) 
-- Load weblogs into HDFS

-- Create fileRDD pointing to weblogs
val fileRDD = sc.textFile("/user/cloudera/weblogs/*.log")
val jpgRDD = fileRDD.filter(_.contains(".jpg"))

//extract IPaddress/UserID from the weblogs for HTML logs

//Answer 1
val fileRDD = sc.textFile("/user/cloudera/weblogs/*.log")
var htmlRDD = fileRDD.filter(_.contains(".html"))
var ipRDD = fileRDD.map(line => (line.split(' ')(0),line.split(' ')(2)))   //The IP RDD is of type <k,v>
ipRDD.take(5).foreach(t => println(t._1 + "/" + t._2))

//Answer 2. More optimal - Possibly compilers can optimize the above
val fileRDD = sc.textFile("/user/cloudera/weblogs/*.log")
var htmlRDD = fileRDD.filter(_.contains(".html"))
val splitRDD = htmlRDD.map(line=>line.split(' '))               //The Split RDD if of type <[]>
splitRDD.take(1).foreach(t=>println(t(0) + "/"+ t(2)))

//Extract the HTML Request alone
val fileRDD = sc.textFile("/user/cloudera/weblogs/*.log")
var htmlRDD = fileRDD.filter(_.contains(".html"))
val splitRDD = htmlRDD.map(line=>line.split(' '))
splitRDD.take(1).foreach(t=>println(t(5)))

//How to find the unique IP address
val fileRDD = sc.textFile("/user/cloudera/weblogs/*.log")
val mapRDD = fileRDD.map(t=>t.split(' '))
 
val ipRDD = mapRDD.map(t=>t(0))
val uidRDD=mapRDD.map(t=>t(2))
val ipUniqueRDD = ipRDD.distinct()
val uniqUIDRDD = uidRDD.distinct()  

ipRDD.count()   // about 1.08 M
ipUniqueRDD.count() //about 474K
uniqUIDRDD.count() //- 35K unique visitors


//Wriggle the Date field out
val fileRDD =  sc.textFile("/user/cloudera/weblogs/*.log")
val splitRDD = fileRDD.map(line=>line.split(' '))
val dateRDD = splitRDD.map(line=>line(3)).map(token=>token.split(':')).map(token=>token(0)).map (token=>token.split('[')).map(token=>token(1)) // Better use Regex
val dateRDD2 =dateRDD.map(date=>date.replace('/','-'))
dateRDD2.count() //This has 1.08M entries

//Similarly wriggle the IPAddress out 
val fileRDD =  sc.textFile("/user/cloudera/weblogs/*.log")
val splitRDD = fileRDD.map(line=>line.split(' '))
var ipRDD =  splitRDD.map(tokens=>tokens(0))
ipRDD.take(1).foreach(println)  
ipRDD.count()   // This has 1.08 entries

//Similarly wriggle out User IDs
val fileRDD =  sc.textFile("/user/cloudera/weblogs/*.log")
val splitRDD = fileRDD.map(line=>line.split(' '))
var userRDD =  splitRDD.map(tokens=>tokens(2))
userRDD.take(1).foreach(println)  
userRDD.count()   // This has 1.08 entries

//Now create an RDD of <date,IP, userID>
val tableRDD=dateRDD.zip(ipRDD)                                                                                             
tableRDD.take(1).foreach(println)

val table2RDD = tableRDD.map(pair=>pair._1+","+pair._2)        
table2RDD.take(1).foreach(println)
val table3RDD = table2RDD .zip(userRDD).map(pair=>pair._1+"," + pair._2)
val table2RDD = tableRDD.map((k)=>(k._1 +",,," + k._2 ))
table3RDD.take(1).foreach(println)


table3RDD.saveAsTextFile("/user/cloudera/weblog_results")



=====Create an external table in Hive and query the same 
create external  table default.weblog_results (date string,ipaddress string,uid int)
ROW FORMAT
              DELIMITED FIELDS TERMINATED BY ','
              LINES TERMINATED BY '\n' STORED AS TEXTFILE
location '/user/cloudera/weblog_results/';

select count(distinct(ipaddress)) from weblog_results;  -- 474798


select date, count(distinct(ipaddress)), count(distinct(uid)) from weblog_results
group by date;  
 


create view weblog_agg_view as 
select date, count(distinct(ipaddress)) ipcnt, count(distinct(uid)) uidcnt from weblog_results
group by date;

//On which date did maximum users visit the web site
select date,max(ipcnt) from weblog_agg_view -- Not working
select  max(ipcnt) from weblog_view -- Ans = 3198
select date from weblog_view where ipcnt = 3198 -- Date is 14/Mar/2014
 
 
// create an aggregate reporting table. Date would be in /user/hive/warehouse/weblog_agg_rpt
create  table weblog_agg_rpt  
ROW FORMAT 
              DELIMITED FIELDS TERMINATED BY ',' 
              LINES TERMINATED BY '\n' STORED AS TEXTFILE 
as select * from weblog_view ;
 


//Visualize the data in Tableau