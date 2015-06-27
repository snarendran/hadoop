//Spark - Loading multiple small files
//Load the data from weblogs from the data directory to HDFS /user/cloudera/weblogs


val tfRDD = sc.textFile("/user/cloudera/weblogs/*log")
val wfRDD = sc.wholeTextFiles("/user/cloudera/weblogs/*log")


// partitions
tfRDD.partitions.length //182 files  = 182 partitions.... too fragmented (~156 MB data)
wfRDD.partitions.length //1 core = 1 partition on sandbox and 2 on cloud
val wmfRDD = sc.wholeTextFiles("/user/cloudera/weblogs/*log",8)  //You control the partitions
wmfRDD.partitions.length //8 partitions

//What is the size of the RDD
tfRDD.count() //About 1M 
wfRDD.count() //About 182


//What is the underlying Data Structure?

tfRDD    // The elements of this RDD is String based. Each element = one line (separated by \n)
wfRDD	 // The elements of this RDD is <String, String>. Each element= "filename", <contents of file>

//Print one element 
tfRDD.take(1).foreach(println) //one line from the first line loaded (could be non-deterministic also)
wfRDD.take(1).foreach(println) //Careful, prints the entire contents of the first file. 


 

//Print the number of lines in each files. Pair is just a nomencleature, you can assign any variable to it. 
wfRDD.take(10).foreach(pair=>println(pair._1))

wfRDD.take(10).foreach(X=>println(X._1))

 