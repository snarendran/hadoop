val sc: SparkContext // An existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

//Version 1.4 - use this syntax
val df = sqlContext.read.json("/user/cloudera/people/people.json").cache()

//Version 1.3 - use this syntax
val df = sqlContext.jsonFile("/user/cloudera/people/people.json").cache()

df.show()

//Ok, display the schema
df.printSchema() //age - long, name - string

//get only the datatypes 
val dftypes = df.dtypes  //returns an Array(String,String) of (colname, datatype) 
dftypes.foreach(s=>println(s._1  ) )  //returns the column name


// Displays the contents of the dataframe
df.show()
df.take(1).foreach(println)
val row = df.head() //Returns an object of type org.apache.spark.sql.row
val row = df.first() // Same as df.head
val rows= df.take(5) //Array of rows

val age = df("age") //Returns an object of type org.apache.spark.sql.row



//ok. enough of row, how about manipulating columns
df.take(1).foreach(s=>println(s(0)))

//Can you print a column from a row?
df.take(1).foreach(s=>println(s("age")))  //Wont work. As you are doing select on Rows
// Rows are just like Arrays, schemaless
https://spark.apache.org/docs/1.1.0/api/scala/index.html#org.apache.spark.sql.api.java.Row

//ok. DFs have schema, so, can I select a column by name?
val names = df.select("name")    // returns a DF!!! not an RDD
names.show()
https://spark.apache.org/docs/1.3.1/api/scala/index.html#org.apache.spark.sql.DataFrame


df.select("name", "age"  ).show()
df.select("name", "age" +1 ).show() //won't work, why? 
df.select("name","age+1").show() //Won't work either...
df.select(df("name"), df("age") + 1).show() //ok, what is the resultant datatype? DF


val fage= df.filter("age >20")  //Works!! Returns DF
val fage= df.select("age+1")    //Must be an implementation issue.... test it in 1.4 !



//Ok, now for some transformations
val c1RDD = df.map(s=>s(0))    //Returns RDD
c1RDD.collect().foreach(println)
 
//Ok, how can i convert name into uppercase
val u = df.map(s=>s.getString(1).toUpperCase)  //Remember each element is of type Row
u.collect().foreach(println)

//Alternatively, you could do this !! 
val u = df.select("name").map(s=>s.getString(0).toUpperCase)
u.collect().foreach(println)
 