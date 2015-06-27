val fileRDD = sc.textFile("/user/cloudera/charles_dickens/")
val wcRDD = fileRDD.map(word=>word.split(' ')).map(fields=>(fields(0),1)).reduceByKey((key1, key2)=>key1+key2)

val sortedRDD = wcRDD.sortByKey(false)
wcRDD.take(10).foreach(println)

