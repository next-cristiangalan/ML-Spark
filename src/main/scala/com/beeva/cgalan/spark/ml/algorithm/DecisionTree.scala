package com.beeva.cgalan.spark.ml.algorithm

import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType

import scala.util.Random
import com.beeva.cgalan.spark.ml.utils.Utils.getTime


/**
  * Created by cristiangalan on 6/07/17.
  */
class DecisionTree(maxDepth : Int = 5, maxBins : Int = 32) extends Ml[DecisionTreeClassificationModel] {

  val decisionTree: DecisionTreeClassifier = new DecisionTreeClassifier().setMaxDepth(maxDepth).setMaxBins(maxBins)
  var model: DecisionTreeClassificationModel = _

  override def train(train: DataFrame) = getTime {
    val Array(trainingData, testData) = train.randomSplit(Array(0.7, 0.3), seed = Random.nextLong())

    // Train a DecisionTree model.
    model = decisionTree.fit(trainingData)
    println("Learned classification tree model:\n" + model.toDebugString)

    // Select (prediction, true label) and compute test error
    test(testData, model)

    model = decisionTree.fit(train)
  }

  override def evaluation(test: DataFrame) = getTime {
    val predictions = model.transform(test)
    predictions.withColumn("prediction", predictions("prediction").cast(IntegerType))
  }

}
