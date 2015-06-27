val fRDD = sc.textFile("/user/cloudera/emp")
val empRDD = fRDD.map (s=>s.split(",")) //contains emp_id,name,dept,salary,dept_code
case class Emp(emp_id:Int,name:String,role:String,salary:Int,dept_code:String)
val empRDD2 = empRDD.map(s=>Emp(s(0).toInt, s(1),s(2),s(3).toInt,s(4))) //RDD[Emp]
val empdf = empRDD2.toDF()

empdf.show()

//Now register employee as a temoporary Database table and use SQL syntax

empdf.registerTempTable("employees")

// SQL statements can be run by using the sql methods provided by sqlContext.
val results = sqlContext.sql("SELECT upper(name),role,salary FROM employees where salary>50000") //results is a DF
results.show()

 
