package com.beeva.cgalan.spark.ml.problems

import com.beeva.cgalan.spark.ml.algorithm.Ml
import com.beeva.cgalan.spark.ml.dataset.{TitanicSchema, TitanicFull, TitanicFullLabel}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._
import org.apache.spark.sql.{DataFrame, Encoders, SaveMode, SparkSession}

/**
  * Created by cristiangalan on 6/07/17.
  */
class Titanic(ss: SparkSession, ml: Ml[_], train: String, test: String) extends Problem {

  import ss.implicits._

  val trainDF: DataFrame = ss.read.option("inferSchema", "true").option("header", "true").csv(train)
  val testDF: DataFrame = ss.read.option("inferSchema", "true").option("header", "true").csv(test)

  def start(): Unit = {
    //Train
    val trainData = TitanicSchema.toDataset(trainDF, Encoders.product[TitanicFullLabel]).map(_.toTitanicTraining).toDF()

    val trainFeatures = transformFeatures(trainData, Some("Survived"))
    trainFeatures.show()

    ml.train(trainFeatures)

    //Test
    val testData = TitanicSchema.toDataset(testDF, Encoders.product[TitanicFull]).map(_.toTitanicTest).toDF()

    val testFeatures = transformFeatures(testData, None)
    val result = ml.evaluation(testFeatures)

    val output = result.withColumnRenamed("prediction", "Survived").select("PassengerId", "Survived")
    output.write.option("header", "true").mode(SaveMode.Overwrite).csv("output.csv")
  }

  def transformFeatures(dataFrame: DataFrame, label: Option[String]): DataFrame = {

    //"Pclass : Converted to binary vector"
    val pClassCat = new OneHotEncoder().setInputCol("Pclass").setOutputCol("PclassCat")

    //"Sex : Converted to binary"
    val sexCat = new StringIndexer().setInputCol("Sex").setOutputCol("SexCat")

    //"Age : Bucketizer"
    val splits = Array(Double.NegativeInfinity, -1, 10, 18, 30, 50, 100)
    val ageBucket = new Bucketizer().setInputCol("Age").setOutputCol("AgeBucket").setSplits(splits)
    val ageCat = new OneHotEncoder().setInputCol("AgeBucket").setOutputCol("AgeCat")

    //SibSp

    //"Parch"
    //"Ticket"
    //"Fare"
    //"Cabin"
    //"Embarked"

    val vectorAssembler = new VectorAssembler().setInputCols(Array("PclassCat", "SexCat", "AgeCat")).setOutputCol("features")

    //Label
    val fittedPipeline = if (label.nonEmpty) {
      val binarizerClassifier = new Binarizer().setInputCol(label.get).setOutputCol("label")

      new Pipeline().setStages(Array(pClassCat, sexCat, ageBucket, ageCat, vectorAssembler, binarizerClassifier)).fit(dataFrame)
    } else new Pipeline().setStages(Array(pClassCat, sexCat, ageBucket, ageCat, vectorAssembler)).fit(dataFrame)

    fittedPipeline.transform(dataFrame)
  }


}
