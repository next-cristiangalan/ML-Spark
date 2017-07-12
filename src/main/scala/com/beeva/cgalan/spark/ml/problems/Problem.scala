package com.beeva.cgalan.spark.ml.problems

import org.apache.spark.sql.DataFrame

/**
  * Created by cristiangalan on 12/07/17.
  */
trait Problem {

  def start(): Unit

  private[ml] def transformFeatures(dataFrame: DataFrame, label: Option[String]): DataFrame
}
