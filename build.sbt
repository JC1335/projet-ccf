name := "Graph_Scala"

version := "1.0"

scalaVersion := "2.12.15"   // ⚠ Spark supporte Scala 2.12.x (pas 2.13 ou 3.x)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql"  % "3.5.0",
  "org.apache.spark" %% "spark-graphx" % "3.5.0"
)
