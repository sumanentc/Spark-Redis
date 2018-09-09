package spark.redis.test

import org.apache.spark.SparkConf
import com.redislabs.provider.redis._
import org.apache.spark.sql.types._


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


    val schema = StructType(
      List(
        StructField("glidepathActualsales", DoubleType, true,Metadata.empty),
        StructField("glidepathTargetsales", DoubleType, true,Metadata.empty),
        StructField("the_date", DateType, true,Metadata.empty),
        StructField("Pipstatus",org.apache.spark.sql.types.StringType, true,Metadata.empty),
        StructField("store", org.apache.spark.sql.types.StringType, true,Metadata.empty),
        StructField("region", org.apache.spark.sql.types.StringType, true,Metadata.empty),
        StructField("division", org.apache.spark.sql.types.StringType, true,Metadata.empty),
        StructField("national", org.apache.spark.sql.types.StringType, true,Metadata.empty),
        StructField("storeClassification", org.apache.spark.sql.types.StringType, true,Metadata.empty),
        StructField("storeOpeningdate", DateType, true,Metadata.empty),
        StructField("storetimezone", org.apache.spark.sql.types.StringType, true,Metadata.empty),
        StructField("storetype", org.apache.spark.sql.types.StringType, true,Metadata.empty)

      )
    )
    import sparkSession.sqlContext.implicits._
    val listRDD = sparkSession.sparkContext.fromRedisSet("temp1")

    val listDS = sparkSession.createDataset(listRDD)
    listDS.createOrReplaceTempView("newstoreglidepath_parque")

    val dataset = sparkSession.sql("select * from newstoreglidepath_parque");
    dataset.collect().foreach(println);

  }

}
