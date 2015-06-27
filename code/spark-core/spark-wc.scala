// Implement the copyFromLocal function - well not quiet
//Load from a local file system and move to HDFS
val fileRDD = sc.textFile("file:/mnt/f/data/charles dickens/*.txt") 
val upperRDD = fileRDD.map(line=>line.toUpperCase )
upperRDD.saveAsTextFile("/user/cloudera/charles_dickensUpper")
