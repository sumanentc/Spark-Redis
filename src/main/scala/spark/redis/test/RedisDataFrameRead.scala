package spark.redis.test

import org.apache.spark.SparkConf

/**
  * Created by suman.das on 10/30/18.
  */
object RedisDataFrameRead {
  def main(args: Array[String]): Unit = {
    val sparkSession = org.apache.spark.sql.SparkSession.builder
      .config(new SparkConf()
        .setMaster("local")
        .setAppName("myApp")
        // initial redis host - can be any node in cluster mode
        .set("redis.host", "localhost")
        // initial redis port
        .set("redis.port", "6379")
        // optional redis AUTH password
        //.set("redis.auth", "")
      ).getOrCreate;

    val loadedDf = sparkSession.read
      .format("org.apache.spark.sql.redis")
      .option("table", "temp")
      .load()
    loadedDf.printSchema()
    loadedDf.show()

    loadedDf.createOrReplaceTempView("newstoreglidepath_parque");

    val grpDataSet = sparkSession.sql("SELECT sum(glidepathActualsales) as glidepathActualsales ,region FROM newstoreglidepath_parque group by region")

    grpDataSet.printSchema();

    grpDataSet.collect().foreach(println);
  }

}
