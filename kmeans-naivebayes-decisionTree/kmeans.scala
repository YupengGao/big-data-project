
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

// Load and parse the data
//val data = sc.textFile("/yxg140730/input/itemusermat")

val data = sc.textFile("/Users/pengpeng/Desktop/itemusermat")
//val splitData = data.map(line=>line.split(' '))
//val IdData = splitData.map(s=>s.map(x=>x(0).toInt))
//val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble).drop(1))).cache()
val parsedData = data.map(s =>(s.split(' ').map(_.toDouble))).cache()
val dataNofirst = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble).drop(1))).cache()
// Cluster the data into two classes using KMeans
val numClusters = 10
val numIterations = 20
val clusters = KMeans.train(dataNofirst, numClusters, numIterations)

// Evaluate clustering by computing Within Set Sum of Squared Errors
//val WSSSE = clusters.computeCost(parsedData)
//println("Within Set Sum of Squared Errors = " + WSSSE)

// Save and load model
//clusters.save(sc, "myModelPath")
//val sameModel = KMeansModel.load(sc, "myModelPath")
//take:Returns the n first elements of this list, or else the whole list, if it has less than n //elements.
//drop:Returns the list without its n first elements. If this list has less than n elements,    //the empty list is returned.
//val predictions = parsedData.map{r => ( r.take(1),clusters.predict(r.drop(1)))}

//predictions.saveAsTextFile("/Users/pengpeng/Desktop/out")

//val allsplit = data.map(line=>line.split(' '))
//val allData = allsplit.map( p => (for(i <- 2 to p.length) p(i).toDouble))

//val parsedData = data.map(s => (s.split(' ').map(_.toDouble))).cache()
//list(n) you can get the nth element in the list
val predictions = parsedData.map{r => 
	val line = Vectors.dense( r.drop(1))
	(r(0).toInt,clusters.predict(line).toString)
}

//val sortprediction = predictions.sortBy(_._2)

val moviedata = sc.textFile("/Users/pengpeng/Desktop/movies.dat")
val movieMap = moviedata.map{line =>
  	val fields = line.split("::")
  	(fields(0).toInt,(fields(1), fields(2)).toString)
}
val joinTwo = predictions.join(movieMap)
//val joinTwo = movieMap.join(combineCluster)
val result = joinTwo.map{line => 
	(line._2._1.toInt,line._1,line._2._2)
}
//sort the prediction according to their cluster and take each 5 point in each cluster
val cluster0 = result.filter(_._1 == 0).take(5)
val cluster1 = result.filter(_._1 == 1).take(5)
val cluster2 = result.filter(_._1 == 2).take(5)
val cluster3 = result.filter(_._1 == 3).take(5)
val cluster4 = result.filter(_._1 == 4).take(5)
val cluster5 = result.filter(_._1 == 5).take(5)
val cluster6 = result.filter(_._1 == 6).take(5)
val cluster7 = result.filter(_._1 == 7).take(5)
val cluster8 = result.filter(_._1 == 8).take(5)
val cluster9 = result.filter(_._1 == 9).take(5)
//union all the point in each cluster
val combineCluster = cluster0.union(cluster1).union(cluster2).union(cluster3).union(cluster4).union(cluster5).union(cluster6).union(cluster7).union(cluster8).union(cluster9)
val distData = sc.parallelize(combineCluster)
val result = distData.saveAsTextFile("/Users/pengpeng/Desktop/out1")