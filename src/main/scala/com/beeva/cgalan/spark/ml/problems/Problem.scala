package com.beeva.cgalan.spark.ml.problems

import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.DataFrame

/**
  * Created by cristiangalan on 12/07/17.
  */
trait Problem {

  def start(): Unit

  def multiStart(): Unit

  private[problems] def pipelineTransform(dataFrame: DataFrame, label: Option[String]): Pipeline
}
