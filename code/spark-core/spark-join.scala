//Join - Does an inner join
val r1 = sc.parallelize( Seq ( (1,2), (3, 4), (3, 6)  ))
val r2 = sc.parallelize(Seq ((3, 9) ) )

val urdd = r1.union(r2)
urdd.collect().foreach(println)

//(3,(4,9)) and (3,(6,9))
val jrdd = r1.join(r2)
jrdd.collect().foreach(println)

//(1,(2,None))
//(3,(4,Some(9)))
//(3,(6,Some(9)))

val lojrdd = r1.leftOuterJoin(r2)
lojrdd.collect().foreach(println)

val rojrdd = r1.rightOuterJoin(r2)
rojrdd.collect().foreach(println)
