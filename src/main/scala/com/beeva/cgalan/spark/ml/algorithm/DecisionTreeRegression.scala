package com.beeva.cgalan.spark.ml.algorithm

import com.beeva.cgalan.spark.ml.utils.Utils.getTime
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, DecisionTreeRegressor}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType

import scala.util.Random

class DecisionTreeRegression(maxDepth : Int = 5, maxBins : Int = 32) extends Ml[DecisionTreeRegressionModel] {

  val decisionTree: DecisionTreeRegressor = new DecisionTreeRegressor().setMaxDepth(maxDepth).setMaxBins(maxBins)
  var model: DecisionTreeRegressionModel = _

  override def train(train: DataFrame) = getTime {
    val Array(trainingData, testData) = train.randomSplit(Array(0.7, 0.3), seed = Random.nextLong())

    // Train a DecisionTree model.
    model = decisionTree.fit(trainingData)
    println("Learned regression tree model:\n" + model.toDebugString)

    // Select (prediction, true label) and compute test error
    test(testData, model)

    model = decisionTree.fit(train)
  }

  override def evaluation(test: DataFrame) = getTime {
    val predictions = model.transform(test)
    predictions.withColumn("prediction", predictions("prediction").cast(IntegerType))
  }

}
