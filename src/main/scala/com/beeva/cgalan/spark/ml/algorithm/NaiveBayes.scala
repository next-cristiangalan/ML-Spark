package com.beeva.cgalan.spark.ml.algorithm

import org.apache.spark.ml
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType

import scala.util.Random

/**
  * Created by cristiangalan on 6/07/17.
  */
class NaiveBayes extends Ml[NaiveBayesModel] {

  var model: NaiveBayesModel = _

  override def train(train: DataFrame): Unit = {
    val Array(trainingData, testData) = train.randomSplit(Array(0.7, 0.3), seed = Random.nextLong())

    // Train a NaiveBayes model.
    model = new ml.classification.NaiveBayes().fit(trainingData)

    // Select (prediction, true label) and compute test error
    test(testData, model)
  }

  override def evaluation(test: DataFrame): DataFrame = {
    val predictions = model.transform(test)
    predictions.withColumn("prediction", predictions("prediction").cast(IntegerType))
  }

}
