name := "scala_LinReg_Estimator"

version := "0.1"

scalaVersion := "2.13.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.2"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.2.2"
libraryDependencies += "org.apache.spark" %% "spark-mllib-local" % "3.2.2"

libraryDependencies  ++= Seq(
  "org.scalanlp" %% "breeze" % "1.2",
  "org.scalanlp" %% "breeze-viz" % "1.2"
)
