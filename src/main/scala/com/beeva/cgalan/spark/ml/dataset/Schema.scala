package com.beeva.cgalan.spark.ml.dataset

import org.apache.spark.sql.{DataFrame, Dataset, Encoder}

/**
  * Created by cristiangalan on 10/07/17.
  */
trait Schema[M] {


  def toDataset[M](dataframe: DataFrame, encoder: Encoder[M]): Dataset[M] = {
    dataframe.na.fill(Double.NegativeInfinity).as[M](encoder)
  }

  def toDataFrame[M](dataset: Dataset[M], colNames: String*): DataFrame = dataset.toDF(colNames: _*)
}
