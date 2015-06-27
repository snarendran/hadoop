val movies = sc.textFile("/user/cloudera/movies/movies").map(s=>s.split(","))   // movieId,title,genres
val links = sc.textFile("/user/cloudera/movies/links").map(s=>s.split(","))     //movieId,imdbId,tmdbId
val ratings = sc.textFile("/user/cloudera/movies/ratings").map(s=>s.split(",")) //userId,movieId,rating,timestamp
val tags = sc.textFile("/user/cloudera/movies/tags").map(s=>s.split(","))       //userId,movieId,tag,timestamp




val rRDD = ratings.map(s=>(s(1).toInt,(s(2).toDouble))).cache()
rRDD.take(5).foreach(println)


//First count how many users have rated every movie
val countRDD = rRDD.map(pair=>(pair._1,1)).reduceByKey((v1,v2)=>(v1+v2)) //8552 unique movies countRDD.count() will reveal that
countRDD.takeOrdered(5).foreach(println)  // For example, 232 users have rated movie ID 1
val crossCheck = rRDD.filter(pair=>pair._1 ==1).count() //should be 232

//Next, sum up all the ratings for each movie
val sumRDD = rRDD.reduceByKey((v1,v2)=>(v1+v2)) // Movie ID 1 has a rating of 888.5
sumRDD.takeOrdered(5).foreach(println)

//An alternative way of achieving the same is 
val fRDD=rRDD.foldByKey(0)( (v1,v2)=>(v1+v2))
fRDD.takeOrdered(5).foreach(println)
 
countRDD.count() // 8552. Has a structure <Key=movieID> <value = # of users who have rated this movie>
sumRDD.count() //both should be 8552.  Has a structure <Key=movieID> <value = sum of ratings>

val jRDD = countRDD.join(sumRDD)   //<key=movieID> value=<#of users, total rating>
jRDD.takeOrdered(5).foreach(println)
val avgRDD = jRDD.map( pair  =>(pair._1, (pair._2._2/pair._2._1)))
avgRDD.takeOrdered(5).foreach(println) 
avgRDD.count() // Always ensure that the count = 8552 across any RDDs that we have manipulated so far

//Now filter for average >=4.0
val goodmovies = avgRDD.filter(s=>s._2>=4.0)
goodmovies.takeOrdered(5).foreach(println) 
goodmovies.count() //There are 1809 movies with ratings of 4.0 or higher)


//Now filter for movies released in 1995 from the moviesRDD
val mRDD=movies.map(s=>(s(0).toInt, s(1))).filter(pair=>(pair._2.contains("(1995)"))) 
mRDD.takeOrdered(5).foreach(println)
mRDD.count() //215 movies were released in 1995

//Okie... Now we are ready to join...Inner Join?
val resultRDD = mRDD.join(goodmovies)
resultRDD.takeOrdered(5).foreach(println)
resultRDD.count()  //33 of them got a rating of 4.0 or higher


//Toy Story did not get a rating of 4.0 or higher??? Crosscheck





(28,(Persuasion (1995),4.066666666666666))
(36,(Dead Man Walking (1995),4.005154639175258))
(37,(Across the Sea of Time (1995),4.5))
(47,(Seven (a.k.a. Se7en) (1995),4.042253521126761))
(67,(Two Bits (1995),4.0))
(77,(Nico Icon (1995),4.0))
(82,(Antonia's Line (Antonia) (1995),4.125))
(110,(Braveheart (1995),4.037671232876712))
(134,(Sonic Outlaws (1995),5.0))
(136,(From the Journals of Jean Seberg (1995),5.0))
(176,(Living in Oblivion (1995),4.125))
(189,(Reckless (1995),5.0))
(194,(Smoke (1995),4.0))
(209,(White Man's Burden (1995),4.0))
(244,(Gumby: The Movie (1995),4.0))
(310,(Rent-a-Kid (1995),4.0))
(394,(Coldblooded (1995),4.0))
(562,(Welcome to the Dollhouse (1995),4.019230769230769))
(632,(Land and Freedom (Tierra y libertad) (1995),5.0))
(633,(Denise Calls Up (1995),4.0))
(638,(Jack and Sarah (1995),5.0))
(645,(Nelly & Monsieur Arnaud (1995),4.5))
(714,(Dead Man (1995),4.119047619047619))
(728,(Cold Comfort Farm (1995),4.15))
(745,(Wallace & Gromit: A Close Shave (1995),4.176470588235294))
(764,(Heavy (1995),4.5))
(816,(Two Deaths (1995),4.0))
(989,(Brother of Sleep (Schlafes Bruder) (1995),4.0))
(1428,(Angel Baby (1995),4.25))
(1757,(Fallen Angels (Duo luo tian shi) (1995),5.0))
(3446,(Funny Bones (1995),5.0))
(6857,(Ninja Scroll (Jûbei ninpûchô) (1995),4.15))
(7669,(Pride and Prejudice (1995),4.75))
