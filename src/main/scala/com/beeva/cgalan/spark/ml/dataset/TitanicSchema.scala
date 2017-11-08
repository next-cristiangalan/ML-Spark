package com.beeva.cgalan.spark.ml.dataset

import org.apache.spark.sql._

/**
  * Created by cristiangalan on 10/07/17.
  */
object TitanicSchema extends Schema[TitanicSchema] {
}

sealed trait TitanicSchema {
  def toCsv: String

  def toTitanicFull: TitanicFull
}

case class TitanicFullLabel(PassengerId: Int, Survived: Double, Pclass: Int, Name: String, Sex: String, Age: Double, SibSp: Int, Parch: Int, Ticket: String, Fare: Double, Cabin: String, Embarked: String) extends TitanicSchema {
  implicit val encoder: Encoder[TitanicFullLabel] = org.apache.spark.sql.Encoders.kryo[TitanicFullLabel]

  override def toCsv: String = List(PassengerId, Survived, Pclass, Name, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked).mkString(",")

  override def toTitanicFull: TitanicFull = TitanicFull(PassengerId, Pclass, Name, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked)
}

case class TitanicFull(PassengerId: Int, Pclass: Int, Name: String, Sex: String, Age: Double, SibSp: Int, Parch: Int, Ticket: String, Fare: Double, Cabin: String, Embarked: String) extends TitanicSchema {
  implicit val encoder: Encoder[TitanicFull] = org.apache.spark.sql.Encoders.kryo[TitanicFull]

  override def toCsv: String = List(PassengerId, Pclass, Name, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked).mkString(",")

  override def toTitanicFull: TitanicFull = this
}