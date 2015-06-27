// Name of the file with device status data

// K is the number of means (center points of clusters) to find
val K = 2

// ConvergeDist -- the threshold "distance" between iterations at which we decide we are done
// Read https://en.wikipedia.org/?title=Decimal_degrees
val convergeDist = .00001


val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val yelpFile="/user/cloudera/yelp/yelp_academic_dataset_business.json"
val ybRDD = sqlContext.jsonFile(yelpFile)

ybRDD.printSchema()
ybRDD.registerTempTable("yelp_bus")

//val pointsRDD = sqlContext.sql("select latitude,longitude,city,state from yelp_bus where city= 'Charlotte' or city='Las Vegas' or city='Phoenix' or city='Edinburgh' or city = 'Karlsruhe' or city='Pittsburgh' or city='Montreal' or city='Waterloo'   or city='Urbana' or city='Champaign'   or city='Madison' ")

val pointsRDD = sqlContext.sql("select latitude,longitude,city,state from yelp_bus where city='Edinburgh' or city = 'Karlsruhe' ")



val points = pointsRDD.map(t=> (t(0).asInstanceOf[Double],t(1).asInstanceOf[Double])).cache()

val kPoints=calculateKMeans(points,K,convergeDist)
	 
   
// Display the final center points        
println("Final K points ============: " )
kPoints.foreach(println)


//val carRDD = pointsRDD.filter(t=> ((t(2).asInstanceOf[String]) =="Carnegie"));


//Not satisfied with the result of k-means cluster. Remove the noise, so that only the top 10 cities are entered



