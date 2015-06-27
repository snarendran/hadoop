val fileRDD = sc.textFile("/user/cloudera/weblogs/*.log")
val mapRDD = fileRDD.map(t=>t.split(' '))
val flatMapRDD=fileRDD.flatMap(t=>t.split(' '))

val ipRDD = mapRDD.map(t=>t(0))
val uidRDD=mapRDD.map(t=>t(2))
val ipUniqueRDD = ipRDD.distinct()
val uniqUIDRDD = uidRDD.distinct() - 35,000 unique users


val pairRDD=mapRDD.map(t=>(t(2),t(0)))
pairRDD.take(1).foreach(println)


val skuFileRDD = sc.textFile("file:/mnt/hgfs/f/data/sku/sku.txt") //input file contains 00001 sku010:sku933:sku022
val mapRDD = skuFileRDD.map(line=>line.split(' ')).map(line=>(line(0),line(1))) //(00001,sku010:sku933:sku022)
val skuRDD = mapRDD.flatMapValues(fields=>fields.split(':') )
skuRDD.collect().foreach(println)


