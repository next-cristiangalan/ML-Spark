package com.beeva.cgalan.spark.ml.algorithm

import org.apache.spark.ml.{Estimator, Pipeline, classification}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType
import com.beeva.cgalan.spark.ml.utils.Utils.getTime
import org.apache.spark.ml
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}

import scala.util.Random

/**
  * Created by cristiangalan on 6/07/17.
  */
class CrossValidation() extends MultiMl[CrossValidatorModel] {

  var cv: CrossValidator = _
  var model: CrossValidatorModel = _

  override def multiTrain(train: DataFrame,  pipeline : Estimator[_], paramGrid : Array[ParamMap]) = getTime {
    val Array(trainingData, testData) = train.randomSplit(Array(0.7, 0.3), seed = Random.nextLong())

    cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    // Train a NaiveBayes model.
    model = cv.fit(trainingData)

    // Select (prediction, true label) and compute test error
    test(testData, model)

    //Complete
    model = cv.fit(train)
  }

  override def train(train: DataFrame) = getTime {
    val Array(trainingData, testData) = train.randomSplit(Array(0.7, 0.3), seed = Random.nextLong())

    // Train a cross validation model.
    model = cv.fit(trainingData)

    // Select (prediction, true label) and compute test error
    test(testData, model)

    //Complete
    model = cv.fit(train)
  }

  override def evaluation(test: DataFrame) = getTime {
    val predictions = model.transform(test)
    predictions.withColumn("prediction", predictions("prediction").cast(IntegerType))
  }

}
