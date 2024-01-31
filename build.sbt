

scalaVersion := "2.13.12"
name := "SparkSQL"
organization := "ch.epfl.scala"
version := "1.0"
val sparkVersion = "3.5.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "9.4.0.jre11"
libraryDependencies += "com.oracle.database.jdbc" % "ojdbc8" % "19.8.0.0"
val javaJDKversion = "11"
javacOptions ++= Seq("-source", javaJDKversion, "-target", javaJDKversion)




