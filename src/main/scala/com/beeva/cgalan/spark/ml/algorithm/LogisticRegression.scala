package com.beeva.cgalan.spark.ml.algorithm

import org.apache.spark.ml
import org.apache.spark.ml.classification
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType

import scala.util.Random

/**
  * Created by cristiangalan on 6/07/17.
  */
class LogisticRegression(maxIter : Int = 100) extends Ml[LogisticRegressionModel] {

  val lr: classification.LogisticRegression = new ml.classification.LogisticRegression()
    .setMaxIter(maxIter)

  var model: LogisticRegressionModel = _

  override def train(train: DataFrame): Unit = {
    val Array(trainingData, testData) = train.randomSplit(Array(0.7, 0.3), seed = Random.nextLong())

    model = lr.fit(trainingData)

    // Print the coefficients and intercept for logistic regression
    println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

    // Select example rows to display
    test(testData, model)

    //Complete
    model = lr.fit(train)
  }

  override def evaluation(test: DataFrame): DataFrame = {
    val predictions = model.transform(test)
    predictions.withColumn("prediction", predictions("prediction").cast(IntegerType))
  }

}
