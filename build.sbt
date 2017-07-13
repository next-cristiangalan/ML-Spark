name := "Ml-Spark"

organization := "Beeva"

name := "Ml-Spark"

version := "1.0"

scalaVersion := "2.11.8"

enablePlugins()

resolvers ++= {
  Seq(
    Resolver.sonatypeRepo("public"),
    "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    "Artima Maven Repository" at "http://repo.artima.com/releases"
  )
}

resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

libraryDependencies ++= {
  Seq(
    "org.apache.spark" %% "spark-core" % "2.2.0" % "compile,test",
    "org.apache.spark" %% "spark-mllib" % "2.2.0" % "compile,test",
    "org.apache.spark" %% "spark-graphx" % "2.2.0" % "compile,test",
    "org.apache.spark" %% "spark-sql" % "2.2.0" % "compile,test",
    "com.esotericsoftware" % "kryo" % "3.0.3",

    //Scala
    "org.scalactic" %% "scalactic" % "3.0.1",
    "com.typesafe.scala-logging" % "scala-logging-slf4j_2.11" % "2.1.2",

    //Test
    "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    "org.mockito" % "mockito-core" % "2.7.22" % "test"
  )
}