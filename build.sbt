name := "spark-redis"

version := "1.0"

scalaVersion := "2.11.6"

val sparkVersion ="2.3.0"

libraryDependencies ++=Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "com.redislabs" %% "spark-redis" % "2.3.0" from "https://oss.sonatype.org/content/groups/staging/com/redislabs/spark-redis/2.3.0/spark-redis-2.3.0.jar"

)
