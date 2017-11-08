package com.beeva.cgalan.spark.ml.algorithm

import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.DataFrame

/**
  * Created by cristiangalan on 6/07/17.
  */
trait MultiMl[M <: Model[M]] extends Ml[M] {

  def multiTrain(train: DataFrame, pipeline: Estimator[_], paramGrid: Array[ParamMap]): (Long, Unit)
}
