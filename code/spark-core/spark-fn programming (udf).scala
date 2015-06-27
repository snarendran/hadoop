// Implement the copyFromLocal function - well not quiet
//Load from a local file system and move to HDFS
def toUpper(s: String): String = { s.toUpperCase }

val fileRDD = sc.textFile("file:/mnt/hgfs/f/data/charles dickens/*.txt").map(toUpper)
 

fileRDD.take(10).foreach(println)