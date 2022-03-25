package com.demo
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._

object unifiedLoader {


  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger(getClass.getName)
    val sparkConf = new SparkConf().setAppName("trueETL")
    val islocal = args(0)
    val table = args(1)
    val data_path = args(2)
    val data_delimiter = args(3)

    if("true".equalsIgnoreCase(islocal))
      sparkConf.setMaster("local[1]")

    val schema =  new StructType()
      .add("id",LongType,true)
      .add("name",StringType,true)
      .add("value",StringType,true)
      .add("timestamp",LongType,true)

    val spark =SparkSession.builder().config(sparkConf)
      .config("spark.hadoop.hive.exec.dynamic.partition","true")
      .config("spark.hadoop.hive.exec.dynamic.partition.mode","nonstrict")
      .enableHiveSupport().getOrCreate()

    val df = spark.read.schema(schema).option("mode","DROPMALFORMED").option("delimiter",data_delimiter).csv(data_path)

    val windowSpec  = Window.partitionBy("id","name")
                      .orderBy(col("timestamp").desc)

    val new_df = df.withColumn("dn_rnk",dense_rank.over(windowSpec))
              .filter("dn_rnk==1")
              .orderBy(col("id"),col("timestamp").desc)
              .drop("timestamp","dn_rnk")

    val mergeExpr = expr("aggregate(data, map(), (acc, i) -> map_concat(acc, i))")
    val finalDF = new_df
              .withColumn("data",map(col("name"),col("value")))
              .groupBy("id").agg(collect_list("data").as("data"))
              .orderBy("id").select(col("id"), mergeExpr.as("settings"))
    finalDF.createOrReplaceTempView("events")
    spark.sql(s"INSERT INTO DEFAULT.AGGREGATED_EVENTS PARTITION (id) SELECT settings,id FROM events")
  }
}
