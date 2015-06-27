// Implement the copyFromLocal function - well not quiet
//Load from a local file system and move to HDFS

val tfRDD = sc.textFile("/user/cloudera/weblogs/*log")
val wfRDD = sc.wholeTextFiles("/user/cloudera/weblogs/*log")



//Convert into Upper
val upperRDD = tfRDD.map(line=>line.toUpperCase )
//upperRDD.saveAsTextFile("/user/cloudera/weblogsUpper")   //Save it in a file 
