package com.beeva.cgalan.spark.ml.algorithm

import org.apache.spark.ml
import org.apache.spark.ml.classification
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType

import scala.util.Random
import com.beeva.cgalan.spark.ml.utils.Utils.getTime

/**
  * Created by cristiangalan on 6/07/17.
  */
class NaiveBayes extends Ml[NaiveBayesModel] {

  val naiveBayes: classification.NaiveBayes = new classification.NaiveBayes()
  var model: NaiveBayesModel = _

  override def train(train: DataFrame) = getTime {
    val Array(trainingData, testData) = train.randomSplit(Array(0.7, 0.3), seed = Random.nextLong())

    // Train a NaiveBayes model.
    model = naiveBayes.fit(trainingData)

    // Select (prediction, true label) and compute test error
    test(testData, model)

    //Complete
    model = naiveBayes.fit(train)
  }

  override def evaluation(test: DataFrame) = getTime {
    val predictions = model.transform(test)
    predictions.withColumn("prediction", predictions("prediction").cast(IntegerType))
  }

}
