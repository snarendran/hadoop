// Implement the copyFromLocal function - well not quiet
//Load from a local file system and move to HDFS
val fileRDD = sc.textFile("file:/mnt/hgfs/f/data/charles dickens/*.txt") 
fileRDD.saveAsTextFile("/user/cloudera/charles_dickens")
