// Test for exception handling in Scala
 
  
  
import org.apache.spark.SparkException
object Test {
   def test {
    val rdd = sc.textFile("/user/cloudera/emp")
	var x = rdd.map(s=>s.split(",")).map(s=>(s(0), s(1)))
    try{
	     x = rdd.map(s=>s.split(",")).map(s=>(s(20), s(10)))
	  } catch {
	    case ex:SparkException=>println("Spark Exception")
	    case _ =>println("Exception")
	  } finally{
		x.take(3).foreach(println)
   }
 }
}

//Run it in REPL
//scala>import Test.test
//scala>test