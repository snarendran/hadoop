Sandbox Setup:
cp /etc/hive/conf/hive-site.xml /etc/spark/conf

vi /usr/lib/spark/bin/compute-classpath.sh (Sandbox) 
// Add the following  line at the end of the file. 
CLASSPATH="$CLASSPATH:/usr/lib/hive/lib/*“
Start spark-shell again

Google Cloud Setup:
Vi  opt/cloudera/parcels/CDH/lib/spark/bin/compute-classpath.sh  
// Add the following  line at the end of the file
CLASSPATH="$CLASSPATH:/usr/lib/hive/lib/*"
// Restart the spark clusters


-------


//Spark Movies

//Load movies, ratings, tags and tags to HDFS under /user/cloudera/<dir> where dir = movies, links, ratings and tags. Ratings were done between April 02, 1996 and March 30, 2015


//Create Hive tables based on the above

CREATE EXTERNAL	 TABLE movies (
        movieId BIGINT, title STRING, genres STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE 
	LOCATION '/user/cloudera/movies/movies';
		
CREATE EXTERNAL	 TABLE links (
        movieId BIGINT, imdbId BIGINT, tmdbId BIGINT)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE 
	LOCATION '/user/cloudera/movies/links';
	
	
//Use BigInt for rating column and see what kind of precision problem occurs... 
CREATE EXTERNAL	 TABLE ratings (
        userId BIGINT, movieId  BIGINT, rating FLOAT, datetime timestamp )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE 
	LOCATION '/user/cloudera/movies/ratings';	
	
CREATE EXTERNAL	 TABLE tags (
        userId BIGINT, movieId  BIGINT, tag String, datetime timestamp )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE 
	LOCATION '/user/cloudera/movies/tags';

	
//movies (movieId,title,genres) - 8570 movies, 706 users (id)
//links (movieId,imdbId,tmdbId)
//ratings (userId,movieId,rating,timestamp)  100,023 ratings
//tags (userId,movieId,tag,timestamp) 2,488 tags


=========== Start spark programming
	
//Now, start your interactive queries in spark
val hc = new org.apache.spark.sql.hive.HiveContext(sc)
import hc.implicits._
hc.cacheTable("movies")
hc.cacheTable("ratings")




val dfmovies = hc.sql("select * from movies")
dfmovies.show()

val dfratings = hc.sql("select * from ratings")
dfratings.show()
 
//Average is not supported yet!! 

//select movieId,(sum(rating)/count(movieId)) as avg, count(movieId), sum(rating) from ratings group by movieId;

val dfavgrating= hc.sql("select movieId as m2,(  sum(rating)/count(movieId) ) as avg from ratings group by movieId")

val dfgoodmovies= dfavgrating.filter(dfavgrating("avg") >=4.0)
dfgoodmovies.show()

//Find out all movies released in 1995
val df1995 = hc.sql("select movieId as m1, title from movies where title like '%(1995)%'");

val resultdf = df1995.join(dfgoodmovies, df1995("m1") === dfgoodmovies("m2"))

resultdf.show()
resultdf.collect().foreach(println)
resultdf.count() //33 movies match the criteria

resultdf.rdd.saveAsTextFile("/user/cloudera/moviesResult")



=====================
[28,Persuasion (1995),28,4.066666666666666]                                     
[36,Dead Man Walking (1995),36,4.005154639175258]
[37,Across the Sea of Time (1995),37,4.5]
[47,Seven (a.k.a. Se7en) (1995),47,4.042253521126761]
[67,Two Bits (1995),67,4.0]
[77,Nico Icon (1995),77,4.0]
[82,Antonia's Line (Antonia) (1995),82,4.125]
[110,Braveheart (1995),110,4.037671232876712]
[134,Sonic Outlaws (1995),134,5.0]
[136,From the Journals of Jean Seberg (1995),136,5.0]
[176,Living in Oblivion (1995),176,4.125]
[189,Reckless (1995),189,5.0]
[194,Smoke (1995),194,4.0]
[209,White Man's Burden (1995),209,4.0]
[244,Gumby: The Movie (1995),244,4.0]
[310,Rent-a-Kid (1995),310,4.0]
[394,Coldblooded (1995),394,4.0]
[562,Welcome to the Dollhouse (1995),562,4.019230769230769]
[632,Land and Freedom (Tierra y libertad) (1995),632,5.0]
[633,Denise Calls Up (1995),633,4.0]
[638,Jack and Sarah (1995),638,5.0]
[645,Nelly & Monsieur Arnaud (1995),645,4.5]
[714,Dead Man (1995),714,4.119047619047619]
[728,Cold Comfort Farm (1995),728,4.15]
[745,Wallace & Gromit: A Close Shave (1995),745,4.176470588235294]
[764,Heavy (1995),764,4.5]
[816,Two Deaths (1995),816,4.0]
[989,Brother of Sleep (Schlafes Bruder) (1995),989,4.0]
[1428,Angel Baby (1995),1428,4.25]
[1757,Fallen Angels (Duo luo tian shi) (1995),1757,5.0]
[3446,Funny Bones (1995),3446,5.0]
[6857,Ninja Scroll (Jûbei ninpûchô) (1995),6857,4.15]
[7669,Pride and Prejudice (1995),7669,4.75]

  