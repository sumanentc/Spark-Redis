package spark.redis.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode

/**
  * Created by suman.das on 10/30/18.
  */
object RedisDataFrameWrite {
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

    val path = "/Users/suman.das/Downloads/newstoreglidepath.csv"
    val base_df = sparkSession.read.option("header","true").csv(path)
    base_df.write
      .format("org.apache.spark.sql.redis")
      .option("table", "temp")
      .mode(SaveMode.Overwrite)
      .save()

  }

}
