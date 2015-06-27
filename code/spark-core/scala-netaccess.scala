//Scala - Net access
val logsRDD = sc.textFile("/user/cloudera/weblogs/*.log").map(line=>line.split(' '))
val actsRDD = sc.textFile("/user/cloudera/accounts/accounts.csv").map(line=>line.split(','))

val uidRDD = logsRDD.map(fields=>(fields(2),1)).reduceByKey((v1,v2)=>(v1+v2))  //Count it. Result is a collection of (uid,cnt)
val actsUIDRDD = actsRDD.map(fields=>(fields(0), fields(4))) //(uid,name)

//join and get the name
val resRDD = uidRDD.join(actsUIDRDD)
val sortedResRDD = resRDD.sortBy( c => c._2,false )
sortedResRDD.take(10).foreach(println) //Result = ID 1903, Hits = 1603, Name= Powell

======
// You can also print by looping thru for loop
for (pair <- sortedResRDD.take(10)) {
   println(pair._1)
}

