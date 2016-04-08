//decision tree
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.Gini

val data = sc.textFile("/Users/pengpeng/Desktop/glass.data")
val parsedData = data.map(s =>(s.split(',').drop(1))).cache()
val Datafinal = parsedData.map{parts=>
	LabeledPoint(parts(9).toInt, Vectors.dense(parts.take(9).map(_.toDouble)))
}
// Split the data into training and test sets (30% held out for testing)
val splits = Datafinal.randomSplit(Array(0.6, 0.4))
val (trainingData, testData) = (splits(0), splits(1))
// Train a DecisionTree model.
//  Empty categoricalFeaturesInfo indicates all features are continuous.
val numClasses = 8
val categoricalFeaturesInfo = Map[Int, Int]()
val impurity = "gini"
val maxDepth = 5
val maxBins = 32

val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,impurity, maxDepth, maxBins)
// Evaluate model on test instances and compute test error
val labelsAndPredictions = testData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val testAccu = labelsAndPredictions.filter(r => r._1 == r._2).count.toDouble / labelsAndPredictions.count



//naive bayes
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

val data = sc.textFile("/Users/pengpeng/Desktop/glass.data")
val parsedData = data.map(s =>(s.split(',').drop(1))).cache()
val Datafinal = parsedData.map{parts=>
	LabeledPoint(parts(9).toInt, Vectors.dense(parts.take(9).map(_.toDouble)))
}

// Split data into training (60%) and test (40%).
val splits = Datafinal.randomSplit(Array(0.6, 0.4), seed = 11L)
val training = splits(0)
val test = splits(1)

val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")

val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count().toDouble / test.count()

