package spark.redis.test

import org.apache.spark.SparkConf
import com.redislabs.provider.redis._

/**
  * Created by suman.das on 9/5/18.
  */
object RedisRead {
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


    val listRDD = sparkSession.sparkContext.fromRedisSet("temp")
    listRDD.collect().foreach(println);
  }

}
