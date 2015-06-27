// Name of the file with device status data
val filename = "/user/cloudera/k-means/devicestatus.txt"

// K is the number of means (center points of clusters) to find
val K = 5

// ConvergeDist -- the threshold "distance" between iterations at which we decide we are done
// Read https://en.wikipedia.org/?title=Decimal_degrees
val convergeDist = .1
    
// Parse the device status data file
// Split by delimiter |
// Parse  latitude and longitude (13th and 14th fields) into pairs
// Filter out records where lat/long is unavailable -- ie: 0/0 points
val points = sc.textFile(filename).
     map(line => line.split('|')).
     map(fields => (fields(12).toDouble,fields(13).toDouble)).
     filter(point => !((point._1 == 0) && (point._2 == 0))).
     cache()
	 
val kPoints=calculateKMeans(points,K,convergeDist)
	 
   
// Display the final center points        
println("Final K points: " )
kPoints.foreach(println)
