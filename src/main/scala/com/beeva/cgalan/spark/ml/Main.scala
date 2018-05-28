package com.beeva.cgalan.spark.ml

import com.beeva.cgalan.spark.ml.algorithm.{CrossValidation, _}
import com.beeva.cgalan.spark.ml.problems.{Problem, Titanic}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}


/**
  * Created by cristiangalan on 6/07/17.
  */
object Main {

  val ARGS: (Int, Int) = (2, 4)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    if (args.length != ARGS._1 && args.length != ARGS._2) {
      println("Add an argument")
      System.exit(-1)
    }

    //Read resources
    val train = if (args.length == ARGS._2) args(ARGS._2 - 2) else getClass.getResource("/train.csv").getPath
    val test = if (args.length == ARGS._2) args(ARGS._2 - 1) else getClass.getResource("/test.csv").getPath

    //Create sparkconf and sparksession
    val sparkConf = new SparkConf().setAppName("ML Training").setMaster("local[2]")

    val ss = SparkSession.builder.config(sparkConf).getOrCreate()

    val ml: Option[Ml[_]] = args(1) match {
      case "logistic" => Some(new LogisticRegression())
      case "naives" => Some(new NaiveBayes())
      case "tree" => Some(new DecisionTree())
      case "binary" => Some(new CrossValidation(,))
      case _ => None
    }

    if (ml.nonEmpty) {
      val value: Problem = args(0) match {
        case "titanic" => new Titanic(ss, ml.get, train, test)
      }

      if(CrossValidation){
        value.multiStart()
      }else
        value.start()
    }else
      println("Add an ML argument")
  }
}