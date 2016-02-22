name := "prediction"

version := "1.0"

scalaVersion := "2.10.4"

val sparkVersion = "1.5.2"

lazy val providedDependencies = Seq(
  "org.apache.spark" %% "spark-core"            % sparkVersion,
  "org.apache.spark" %% "spark-sql"             % sparkVersion,
  "org.apache.spark" %% "spark-mllib"           % sparkVersion,
  "org.apache.spark" %% "spark-streaming"       % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion
)

libraryDependencies ++= providedDependencies

libraryDependencies ++= Seq(
  "com.databricks" %% "spark-csv" % "1.3.0"
)
