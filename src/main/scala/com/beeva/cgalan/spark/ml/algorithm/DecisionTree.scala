package com.beeva.cgalan.spark.ml.algorithm

import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType

import scala.util.Random

/**
  * Created by cristiangalan on 6/07/17.
  */
class DecisionTree extends Ml[DecisionTreeClassificationModel] {

  var model: DecisionTreeClassificationModel = _

  override def train(train: DataFrame): Unit = {
    val Array(trainingData, testData) = train.randomSplit(Array(0.7, 0.3), seed = Random.nextLong())

    // Train a DecisionTree model.
    model = new DecisionTreeClassifier().fit(trainingData)
    println("Learned classification tree model:\n" + model.toDebugString)

    // Select (prediction, true label) and compute test error
    test(testData, model)
  }

  override def evaluation(test: DataFrame): DataFrame = {
    val predictions = model.transform(test)
    predictions.withColumn("prediction", predictions("prediction").cast(IntegerType))
  }

}
