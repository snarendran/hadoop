// Open spark-shell and run the following commands
//This command opens data from a file. Try multiple files, wildcards

val fileRDD = sc.textFile("user/cloudera/charles dickens/*.txt") 


//Note that it returns immediately. It does NOT read the data yet -> only a shell of an RDD is built
// val - static variable; var = variable; can be reassigned. 
// Try to load from HDFS

//Do a wc-l (line count)
fileRDD.count()