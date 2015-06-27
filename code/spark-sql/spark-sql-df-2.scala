val fRDD = sc.textFile("/user/cloudera/emp")
val empRDD = fRDD.map (s=>s.split(",")) //contains emp_id,name,dept,salary,dept_code
case class Emp(emp_id:Int,name:String,role:String,salary:Int,dept_code:String)
val empRDD2 = empRDD.map(s=>Emp(s(0).toInt, s(1),s(2),s(3).toInt,s(4))) //RDD[Emp]
val empdf = empRDD2.toDF()

empdf.show()

empdf.filter("salary>50000").show()

val grdata = empdf.groupBy("role","dept_code" ) //Not a DF, but is of type org.apache.spark.sql.GroupedData

grdata.count().show()
// Api for Grouped Data: https://spark.apache.org/docs/1.3.1/api/scala/index.html#org.apache.spark.sql.GroupedData