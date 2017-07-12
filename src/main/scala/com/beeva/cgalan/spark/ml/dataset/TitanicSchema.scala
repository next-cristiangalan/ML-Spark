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

  def toTitanicTraining: TitanicTraining

  def toTitanicTest: TitanicTest
}

case class TitanicFullLabel(PassengerId: Int, Survived: Double, Pclass: Int, Name: String, Sex: String, Age: Double, SibSp: Int, Parch: Int, Ticket: String, Fare: Double, Cabin: String, Embarked: String) extends TitanicSchema {
  implicit val encoder: Encoder[TitanicFullLabel] = org.apache.spark.sql.Encoders.kryo[TitanicFullLabel]

  override def toCsv: String = List(PassengerId, Survived, Pclass, Name, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked).mkString(",")

  override def toTitanicFull: TitanicFull = TitanicFull(PassengerId, Pclass, Name, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked)

  override def toTitanicTraining: TitanicTraining = TitanicTraining(Survived, Pclass, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked)

  override def toTitanicTest: TitanicTest = TitanicTest(PassengerId: Int, Pclass, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked)
}

case class TitanicFull(PassengerId: Int, Pclass: Int, Name: String, Sex: String, Age: Double, SibSp: Int, Parch: Int, Ticket: String, Fare: Double, Cabin: String, Embarked: String) extends TitanicSchema {
  implicit val encoder: Encoder[TitanicFull] = org.apache.spark.sql.Encoders.kryo[TitanicFull]

  override def toCsv: String = List(PassengerId, Pclass, Name, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked).mkString(",")

  override def toTitanicFull: TitanicFull = this

  override def toTitanicTraining: TitanicTraining = TitanicTraining(-1, Pclass, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked)

  override def toTitanicTest: TitanicTest = TitanicTest(PassengerId, Pclass, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked)
}

case class TitanicTraining(Survived: Double, Pclass: Int, Sex: String, Age: Double, SibSp: Int, Parch: Int, Ticket: String, Fare: Double, Cabin: String, Embarked: String) extends TitanicSchema {
  implicit val encoder: Encoder[TitanicTraining] = org.apache.spark.sql.Encoders.kryo[TitanicTraining]

  override def toCsv: String = List(Survived, Pclass, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked).mkString(",")

  override def toTitanicFull: TitanicFull = TitanicFull(-1, Pclass, "", Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked)

  override def toTitanicTraining: TitanicTraining = this

  override def toTitanicTest: TitanicTest = TitanicTest(-1, Pclass, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked)
}

case class TitanicTest(PassengerId: Int, Pclass: Int, Sex: String, Age: Double, SibSp: Int, Parch: Int, Ticket: String, Fare: Double, Cabin: String, Embarked: String) extends TitanicSchema {
  implicit val encoder: Encoder[TitanicTest] = org.apache.spark.sql.Encoders.kryo[TitanicTest]

  override def toCsv: String = List(Pclass, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked).mkString(",")

  override def toTitanicFull: TitanicFull = TitanicFull(-1, Pclass, "", Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked)

  override def toTitanicTraining: TitanicTraining = TitanicTraining(-1, Pclass, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked)

  override def toTitanicTest: TitanicTest = this
}
