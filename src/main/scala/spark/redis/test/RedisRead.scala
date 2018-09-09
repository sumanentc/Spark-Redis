package spark.redis.test

import org.apache.spark.SparkConf
import com.redislabs.provider.redis._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import com.github.nscala_time.time.Imports._


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
        StructField("the_date", org.apache.spark.sql.types.StringType, true,Metadata.empty),
        StructField("Pipstatus",org.apache.spark.sql.types.StringType, true,Metadata.empty),
        StructField("store", org.apache.spark.sql.types.StringType, true,Metadata.empty),
        StructField("region", org.apache.spark.sql.types.StringType, true,Metadata.empty),
        StructField("division", org.apache.spark.sql.types.StringType, true,Metadata.empty),
        StructField("national", org.apache.spark.sql.types.StringType, true,Metadata.empty),
        StructField("storeClassification", org.apache.spark.sql.types.StringType, true,Metadata.empty),
        StructField("storeOpeningdate", org.apache.spark.sql.types.StringType, true,Metadata.empty),
        StructField("storetimezone", org.apache.spark.sql.types.StringType, true,Metadata.empty),
        StructField("storetype", org.apache.spark.sql.types.StringType, true,Metadata.empty)

      )
    )

    def row(line: List[String]): Row = Row(line(0).toDouble, line(1).toDouble,line(2),line(3),line(4),line(5),line(6),line(7),line(8),line(9),line(10),line(11))

    //import sparkSession.sqlContext.implicits._
    val listRDD = sparkSession.sparkContext.fromRedisSet("temp1").map(_.split(",").to[List]).map(row)

    //val listDS = sparkSession.createDataset(listRDD)
    val listDataFrame = sparkSession.createDataFrame(listRDD,schema);
    listDataFrame.createOrReplaceTempView("newstoreglidepath_parque")

    val dataRDD = sparkSession.sql("select * from newstoreglidepath_parque")
    dataRDD.printSchema();
    dataRDD.collect().foreach(println);

    val grpDataSet = sparkSession.sql("SELECT sum(glidepathActualsales) as glidepathActualsales ,region FROM newstoreglidepath_parque group by region")

    grpDataSet.printSchema();

    grpDataSet.collect().foreach(println);

  }

}
