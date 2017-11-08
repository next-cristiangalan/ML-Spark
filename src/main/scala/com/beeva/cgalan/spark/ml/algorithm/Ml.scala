package com.beeva.cgalan.spark.ml.algorithm

import org.apache.spark.ml
import org.apache.spark.ml.Model
import org.apache.spark.sql.DataFrame
import com.beeva.cgalan.spark.ml.utils.Utils.getTime

/**
  * Created by cristiangalan on 6/07/17.
  */
trait Ml[M <: Model[M]] {

  def train(train: DataFrame) : (Long, Unit)

  def test(test: DataFrame, model: Model[M]) : (Long, Unit) = getTime {
    // Select example rows to display.
    val predictions = model.transform(test)

    println("Predictions:")
    val predictions2 = predictions.drop("rawPrediction", "features").where("label != prediction")
    predictions2.show(predictions2.count().toInt)

    // Select (prediction, true label) and compute test error
    val evaluator = new ml.evaluation.MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Test set accuracy = " + accuracy)
  }

  def evaluation(eval: DataFrame): (Long, DataFrame)
}
