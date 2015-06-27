 
val fileRDD = sc.textFile("user/cloudera/2cities/")  
val filterRDD = fileRDD.filter(line=> line.startsWith ("I")  )              // Use of RegEx recommended. 
filterRDD.take(10).foreach(println) //Test it by printing locally
 