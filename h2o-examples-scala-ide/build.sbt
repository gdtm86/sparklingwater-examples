name := "ml-examples"

version := "1.0"

scalaVersion := "2.10.6"


libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "2.6.0-mr1-cdh5.5.1" % "provided" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.spark" %% "spark-core" % "1.5.0-cdh5.5.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.5.0-cdh5.5.1"  % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.5.0-cdh5.5.1" % "provided",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "ai.h2o" %% "sparkling-water-core" % "1.5.14",
  "com.databricks" %% "spark-csv" % "1.4.0"
)

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Maven Central Server" at "http://repo1.maven.org/maven2",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos"
)