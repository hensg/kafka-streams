name := "spark-kafka-processor"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3" 

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"

libraryDependencies += "org.apache.spark" %%  "spark-sql-kafka-0-10" % "2.4.3"