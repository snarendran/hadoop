//spark-sql dataframe

 


//Configuration. Tested on Sandbox only

// cp /etc/hive/conf/hive-site.xml /etc/spark/conf
// vi /usr/lib/spark/bin/compute-classpath.sh (Sandbox) or opt/cloudera/parcels/CDH/lib/spark/bin/compute-classpath.sh in case of GC
// Add the following  
//    CLASSPATH="$CLASSPATH:/usr/lib/hive/lib/*"
// Restart spark clusters

-------



val hc = new org.apache.spark.sql.hive.HiveContext(sc)
import hc.implicits._

val ct=hc.table("comments")   
val df = hc.sql("Select count(*) from comments")                   //1,113,651 records vs 1,360,390 242MB  in disk vs. 26 MB in memory
val df = hc.sql("Select * from comments").cache()
val df_cache = hc.sql("Select * from comments").cache()		


df.take(100).foreach(println)   //only now does the job runs -> Goto http://localhost:4040/jobs/  : Took 0.8 sec
df_cache.take(100).foreach(println)    // Took a while 27sec
df_cache.count() // Took 19 sec
df_cache.count() //Running again -took only 0.2 seconds
val mi_df = df_cache.filter($"state".contains("MI")) //Filter for state = MI 
mi_df.select ("city").show() //Show all cities in MI


//Alternatively cache and uncache the entire table as follows
hc.cacheTable("comments")
val all_states = hc.sql("select * from comments")

val cities= hc.sql("select * from comments where city='kalamazoo' and state = 'MI' ") // Will show nothing due to a data bug. use the next syntax
val cities = hc.sql("select * from comments where city like '%Kalamazoo%' and state like '%MI%' ")
cities.show() //print all values for cities

 
val res_df = hc.sql("select streetnumber,streetname, a.city, latitude from zip a ,comments2 b where a.city = 'Boston'  and  a.city like b.city    ").cache()
 