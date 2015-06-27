// A massively scabale Minus query?


val hc = new org.apache.spark.sql.hive.HiveContext(sc)
import hc.implicits._
hc.cacheTable("comments2")
hc.cacheTable("comments")


val lminusdf= hc.sql(" SELECT  a.* FROM comments a LEFT JOIN comments2 b  ON  a.CHANGESRCE = b.CHANGESRCE  AND a.CHANGEDATE = b.CHANGEDATE  AND a.ADDRESS2 = b.ADDRESS2 WHERE b.changesrce is null")

lminusdf.count() //1,016,694


val rminusdf= hc.sql(" SELECT  a.* FROM comments2 a LEFT JOIN comments b  ON  a.CHANGESRCE = b.CHANGESRCE  AND a.CHANGEDATE = b.CHANGEDATE  AND a.ADDRESS2 = b.ADDRESS2 WHERE b.changesrce is null")

rminusdf.count() // 1 KurekXX


val cmtdf = hc.sql("select count(*) from comments")
val cmt2df = hc.sql("select count(*) from comments2")

cmtdf.show() //1,113,651
cmt2df.show() //10,515

//Do the math: 1,016,694 - 10,515 =  10,515

//Now, run a minus query on 1Mx1M data set. Do not run it on your sandbox. Try it on Google Cloud
val lminusdf= hc.sql(" SELECT  a.* FROM comments a LEFT JOIN comments b  ON  a.CHANGESRCE = b.CHANGESRCE  AND a.CHANGEDATE = b.CHANGEDATE  AND a.ADDRESS2 = b.ADDRESS2 WHERE b.changesrce is null")
