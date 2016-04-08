import breeze.linalg._
import org.apache.spark.HashPartitioner
//spark-shell -i als.scala to run this code
//SPARK_SUBMIT_OPTS="-XX:MaxPermSize=4g" spark-shell -i als.scala
//UserID::MovieID::Rating::Timestamp

//Implementation of sec 14.3 Distributed Alternating least squares from stanford Distributed Algorithms and Optimization tutorial. 

//loads ratings from file


val ratings = sc.textFile("/Users/pengpeng/Desktop/ratings.dat").map(l => (l.split("::")(0),l.split("::")(1),l.split("::")(2))) 

// counts unique movies
val itemCount = ratings.map(x=>x._2).distinct.count 

// counts unique user
val userCount = ratings.map(x=>x._1).distinct.count 

// get distinct movies
val items = ratings.map(x=>x._2).distinct   

// get distinct user
val users = ratings.map(x=>x._1).distinct  

// latent factor
val k= 5  

//create item latent vectors
val itemMatrix = items.map(x=> (x,DenseVector.zeros[Double](k)))   
//Initialize the values to 0.5
// generated a latent vector for each item using movie id as key Array((movie_id,densevector)) e.g (2,DenseVector(0.5, 0.5, 0.5, 0.5, 0.5)
var myitemMatrix = itemMatrix.map(x => (x._1,x._2(0 to k-1):=0.5)).partitionBy(new HashPartitioner(10)).persist  

//create user latent vectors
val userMatrix = users.map(x=> (x,DenseVector.zeros[Double](k)))
//Initialize the values to 0.5
// generate latent vector for each user using user id as key Array((userid,densevector)) e.g (2,DenseVector(0.5, 0.5, 0.5, 0.5, 0.5)
var myuserMatrix = userMatrix.map(x => (x._1,x._2(0 to k-1):=0.5)).partitionBy(new HashPartitioner(10)).persist 

// group rating by items. Elements of type org.apache.spark.rdd.RDD[(String, (String, String))] (itemid,(userid,rating)) e.g  (1,(2,3))
val ratingByItem = sc.broadcast(ratings.map(x => (x._2,(x._1.toDouble,x._3.toDouble)))) 

// group rating by user.  Elements of type org.apache.spark.rdd.RDD[(String, (String, String))] (userid,(item,rating)) e.g  (1,(3,5)) 
val ratingByUser = sc.broadcast(ratings.map(x => (x._1,(x._2,x._3)))) 
//Broadcast variables allow the programmer to keep a read-only variable cached on each machine rather than shipping a copy of it with tasks. 
//They can be used, for example, to give every node a copy of a large input dataset in an efficient manner
var i =0
for( i <- 1 to 10){
	// regularization factor which is lambda.
	val regfactor = 1.0 
	val regMatrix = DenseMatrix.zeros[Double](k,k)  //generate an diagonal matrix with dimension k by k
	//filling in the diagonal values for the reqularization matrix.
	regMatrix(0,::) := DenseVector(regfactor,0,0,0,0).t 
	regMatrix(1,::) := DenseVector(0,regfactor,0,0,0).t 
	regMatrix(2,::) := DenseVector(0,0,regfactor,0,0).t 
	regMatrix(3,::) := DenseVector(0,0,0,regfactor,0).t 
	regMatrix(4,::) := DenseVector(0,0,0,0,regfactor).t   

//===========================================Homework 4. Implement code to calculate equation 2 and 3 .===================================================
//=================You will be required to write code to update the myuserMatrix which contains the latent vectors for each user and myitemMatrix which is the matrix that contains the latent vector for the items
//Please Fill in your code here.
	
	// val result = myitemMatrix.map{x=>
	// 	val matrix = x._2*x._2.t
	// 	tempMatrix = tempMatrix + matrix
	// 	(x._1,tempMatrix)
		
	// }

	//******updata X********

	//Join Ratings with Y factors using key i (items)
	val joinTwo = myitemMatrix.join(ratings.map(x => (x._2,(x._1,x._3))))
	//Map to compute yiyi' and change key to u (user)
	val user_matrixA = joinTwo.map{x=>
		val matrix = x._2._1*x._2._1.t
		(x._2._2._1,matrix)
	}
	val user_matrixB = joinTwo.map{x=>
		val temp = x._2._1*x._2._2._2.toDouble
		(x._2._2._1,temp)
	}
	//ReduceByKey u (user) to compute sum yiyi'
	val matrixA = user_matrixA.reduceByKey(_+_)
	val matrixB = user_matrixB.reduceByKey(_+_)
	val matrixLeft = matrixA.map{x=>
		(x._1,x._2+regfactor*regMatrix)
	}
	//invert
	val invertMatrix = matrixLeft.map(x=>(x._1,inv(x._2)))
	//Another ReduceByKey u(user) to compute SUMruiyi
	val matrixResult = matrixB.join(invertMatrix)
	myuserMatrix = matrixResult.map{x=>
		val re = x._2._2*x._2._1
		(x._1,re)
	}
	//******updata Y********
	//join by user
	val joinTwoY = myuserMatrix.join(ratings.map(x => (x._1,(x._2,x._3))))
	//Map to compute XiXi' and change key to i (item)
	val item_matrixA = joinTwoY.map{x=>
		val matrix = x._2._1*x._2._1.t
		(x._2._2._1,matrix)
	}
	val item_matrixB = joinTwoY.map{x=>
		val temp = x._2._1*x._2._2._2.toDouble
		(x._2._2._1,temp)
	}
	//ReduceByKey u (user) to compute sum yiyi'
	val matrixAY = item_matrixA.reduceByKey(_+_)
	val matrixBY = item_matrixB.reduceByKey(_+_)
	val matrixLeftY = matrixAY.map{x=>
		(x._1,x._2+regfactor*regMatrix)
	}
	//invert
	val invertMatrixY = matrixLeftY.map(x=>(x._1,inv(x._2)))
	//Another ReduceByKey u(user) to compute SUMruiyi
	val matrixResultY = matrixBY.join(invertMatrixY)
	myitemMatrix = matrixResultY.map{x=>
		val re = x._2._2*x._2._1
		(x._1,re)
	}
//==========================================End of update latent factors=================================================================
}
//======================================================Implement code to recalculate the ratings a user will give an item.====================

//Hint: This requires multiplying the latent vector of the user with the latent vector of the  item. Please take the input from the command line. and
// Provide the predicted rating for user 1 and item 914, user 1757 and item 1777, user 1759 and item 231.

//Your prediction code here

val filteredUser = myuserMatrix.filter(line=>(line._1=="1"||line._1=="1757"||line._1=="1759"))
val filteredItem = myitemMatrix.filter(line=>(line._1=="914"||line._1=="1777"||line._1=="231"))
val User = myuserMatrix.filter(line=>(line._1=="1")).map(x=>(1,x._2))
val Item = myitemMatrix.filter(line=>(line._1=="914")).map(x=>(1,x._2))
val result1  = User.join(Item).map(x=>((1,914),x._2._1.t*x._2._2))
//***********
val User = myuserMatrix.filter(line=>(line._1=="1757")).map(x=>(1,x._2))
val Item = myitemMatrix.filter(line=>(line._1=="1777")).map(x=>(1,x._2))
val result2  = User.join(Item).map(x=>((1757,1777),x._2._1.t*x._2._2))

val User = myuserMatrix.filter(line=>(line._1=="1759")).map(x=>(1,x._2))
val Item = myitemMatrix.filter(line=>(line._1=="231")).map(x=>(1,x._2))
val result3  = User.join(Item).map(x=>((1759,231),x._2._1.t*x._2._2))
