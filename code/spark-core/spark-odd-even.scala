val a = sc.parallelize(1 to 100)

val oddEvenRDD = a.groupBy   (number => {
	if (number % 2 == 0) "odd" else "even" 
})

val odd = oddEvenRDD.take(1)
val even = oddEvenRDD.take(2).drop(1) //inefficient, but ok... zipWithIndex is also inefficient !! 
 
 
//can you alternatively print upper and lower?
 
val rdd = sc.textFile("/user/cloudera/emp/")

def myCase(line:String,i:Int):String= { if (i %2==0) return line.toUpperCase else return line.toLowerCase}

var i=0
rdd.take(10).foreach( line=> { i=i+1; println(myCase (line,i)) } )


//But, wait, the above is done on the driver side!!  Can you do it on the RDD?

val crdd = rdd.map(s=> {
	i=i+1;
	myCase(s,i)
})