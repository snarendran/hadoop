val rdd = sc.textFile("/user/cloudera/emp/")

val rdd2 = rdd.map(s=>s.split(",")).map(s=>(s(0),s(1)))
val rdd3 = rdd2.groupByKey()   //A pair RDD
rdd3.take(10).foreach(println)

val rdd4 = rdd.map(s=>s.split(",")).map(s=>(s(0),s(1),1))
val rdd5 = rdd4.reduceByKey(v1,v2=>v1+v2)   // RDD4 is NOT a pair RDD. reduceByKey is Not Available
rdd4.take(1).foreach(println)



val rdd6 = rdd.map(s=>s.split(",")).map(s=> (  (s(0),(s(1))),1) )
val rdd7= rdd6.reduceByKey((v1,v2)=>(v1+v2) )   //A Pair RDD
rdd7.take(1).foreach(println) 


val rdd8 = rdd.map(s=>s.split(",")).map(s=> (  (s(0),(s(1),1) )) )
val rdd9= rdd8.reduceByKey( (v1,v2)=>(v1._2+ v2._2) )   //A Pair RDD, but you can't reduce it like this
 
 
//Sort By Key
srdd.take(1).foreach(println)
val srdd= rdd2.sortByKey(false)
srdd.take(1).foreach(println)

//Union 
val urdd = rdd2.union(rdd2)
urdd.collect().foreach(println)

//Group By
val grdd = urdd.groupByKey()
grdd.collect().foreach(println)





