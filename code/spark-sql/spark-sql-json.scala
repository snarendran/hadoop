val sc: SparkContext // An existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

//Version 1.4 - use this syntax
val df = sqlContext.read.json("/user/cloudera/people/people.json").cache()

//Version 1.3 - use this syntax
val df = sqlContext.jsonFile("/user/cloudera/people/people.json").cache()

df.show()
 