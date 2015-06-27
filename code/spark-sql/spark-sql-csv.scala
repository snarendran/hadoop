
download and build the package from databricks
git clone https://github.com/databricks/spark-csv
cd spark-csv
sbt package  

//Takes a while to build. You also need commons-csv jar file which are in external jars directory

//Instructions for Sandbox
cp </path/from/spark-csv_2.11-1.1.0.jar> /usr/lib/spark/lib 
// vi /usr/lib/spark/bin/compute-classpath.sh (Sandbox) and add the following line
CLASSPATH="$CLASSPATH:/usr/lib/spark/lib/*"

//Instructions for GC
cp </path/from/spark-csv_2.11-1.1.0.jar> /opt/cloudera/parcels/CDH/usr/lib/spark/lib
// vi /opt/cloudera/parcels/CDH/spark/bin/compute-classpath.sh and add the following line
CLASSPATH="$CLASSPATH:/usr/lib/spark/lib/*




import org.apache.spark.sql.SQLContext

//1.4 API
val sqlContext = new SQLContext(sc)
val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("/user/cloudera/cars/cars.csv")
df.select("year", "model").write.format("com.databricks.spark.csv").save("newcars.csv")

//1.3 API
import org.apache.spark.sql.SQLContext

import com.databricks.spark.csv._

val sqlContext = new SQLContext(sc)
val df = sqlContext.load("com.databricks.spark.csv", Map("path" -> "/user/cloudera/cars/cars.csv", "header" -> "true"))
df.select("year", "model").save("newcars.csv", "com.databricks.spark.csv")


