// Implement the copyFromLocal function - well not quiet
//Load from a local file system and move to HDFS

val tfRDD = sc.textFile("/user/cloudera/weblogs/*log")
val wfRDD = sc.wholeTextFiles("/user/cloudera/weblogs/*log")



//Convert into Upper
val upperRDD = tfRDD.map(line=>line.toUpperCase )
//upperRDD.saveAsTextFile("/user/cloudera/weblogsUpper")   //Save it in a file 


//Now, try to do the same with the whole file data set. What happens?
val wupperRDDv= wfRDD.map(file=>file.toUpperCase) //Error !!
val wupperRDDv= wfRDD.mapValues(file=>file.toUpperCase) 
wupperRDDv.take(1).foreach() // It converts the values, but keeps the key as is
wupperRDDv.take(1).foreach(pair=>println (pair._2))


//Now, can I convert the key also? 
val wupperRDDk = wfRDD.mapKey(fileName=>fileName.toUpperCase)  //Not available
val wupperRDDk = wfRDD.keys.map(fileName=>fileName.toUpperCase)  
wupperRDDk.take(1).foreach(pair=>println (pair))


//Now, I want to print the key and the value both in capitals
val jRDD = wupperRDDk.zip(wupperRDDv.values)
jRDD.take(1).foreach(pair=>(println(pair._1, pair._2)))