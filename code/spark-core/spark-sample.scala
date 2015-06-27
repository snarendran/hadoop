//Sample

val rdd = sc.textFile("/user/cloudera/emp/")

val srdd = rdd.sample(false,0.1) //will give you ~10% of the dataset
srdd.collect().foreach(println) 

val srdd = rdd.sample(true,0.1) //with replacement, so chances are you would get the same element !!  Repeat the tests
srdd.collect().foreach(println) 
