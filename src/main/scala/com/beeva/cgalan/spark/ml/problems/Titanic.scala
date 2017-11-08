package com.beeva.cgalan.spark.ml.problems

import com.beeva.cgalan.spark.ml.algorithm.{CrossValidation, Ml, MultiMl}
import com.beeva.cgalan.spark.ml.dataset.{TitanicFull, TitanicFullLabel, TitanicSchema}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.{DataFrame, Encoders, SaveMode, SparkSession}

/**
  * Created by cristiangalan on 6/07/17.
  */
class Titanic(ss: SparkSession, ml: Ml[_], train: String, test: String) extends Problem {

  val trainDF: DataFrame = ss.read.option("inferSchema", "true").option("header", "true").csv(train)
  val testDF: DataFrame = ss.read.option("inferSchema", "true").option("header", "true").csv(test)

  def start(): Unit = {
    //Train
    val trainData = TitanicSchema.toDataset(trainDF, Encoders.product[TitanicFullLabel]).toDF().drop("PassengerId", "Name")

    val trainFeatures = pipelineTransform(trainData, Some("Survived")).fit(trainData).transform(trainData)
    trainFeatures.show()

    ml.train(trainFeatures)

    //Test
    val testData = TitanicSchema.toDataset(testDF, Encoders.product[TitanicFull]).toDF().drop("Name")

    val testFeatures   = pipelineTransform(testData, None).fit(testData).transform(testData)
    val result = ml.evaluation(testFeatures)

    val output = result._2.withColumnRenamed("prediction", "Survived").select("PassengerId", "Survived")
    output.write.option("header", "true").mode(SaveMode.Overwrite).csv("output.csv")
  }

  override def multiStart(): Unit = {
    //Train
    val trainData = TitanicSchema.toDataset(trainDF, Encoders.product[TitanicFullLabel]).toDF().drop("PassengerId", "Name")

    val pipeline = pipelineTransform(trainData, Some("Survived"))
    val paramGrid =

      ml.asInstanceOf[MultiMl[CrossValidation]].multiTrain(trainData, pipeline, paramGrid)

    //Test
    val testData = TitanicSchema.toDataset(testDF, Encoders.product[TitanicFull]).toDF().drop("Name")

    val pipelineFeatures = pipelineTransform(testData, None)
    val testFeatures = pipelineFeatures.transform(testData)
    val result = ml.evaluation(testFeatures)

    val output = result._2.withColumnRenamed("prediction", "Survived").select("PassengerId", "Survived")
    output.write.option("header", "true").mode(SaveMode.Overwrite).csv("output.csv")

  }


  private[problems] def pipelineTransform(label: Option[String]): Pipeline = {

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

    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")



    //"Embarked"
    val embarkedIndex = new StringIndexer().setHandleInvalid("keep").setInputCol("Embarked").setOutputCol("EmbarkedIndex")
    val embarkedCat = new OneHotEncoder().setInputCol("EmbarkedIndex").setOutputCol("EmbarkedCat")

    //Assembler
    val vectorAssembler = new VectorAssembler().setInputCols(Array("PclassCat", "SexCat", "AgeCat", "EmbarkedCat")).setOutputCol("features")

    //Label
    if (label.nonEmpty) {
      val binarizerClassifier = new Binarizer().setInputCol(label.get).setOutputCol("label")
      new Pipeline().setStages(Array(pClassCat, sexCat, ageBucket, ageCat, embarkedIndex, embarkedCat, vectorAssembler, binarizerClassifier))
    } else {
      new Pipeline().setStages(Array(pClassCat, sexCat, ageBucket, ageCat, embarkedIndex, embarkedCat, vectorAssembler))
    }
  }
}
