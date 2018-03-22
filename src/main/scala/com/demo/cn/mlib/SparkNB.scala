package com.demo.cn.mlib

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object SparkNB {

  val structFields = List(StructField("score",DoubleType),StructField("comments",StringType))
  val types = StructType(structFields)

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("clusters")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val rdd=sc.textFile("file:///E:\\file\\pinlun.csv")
    val rowRdd = rdd.map(line=>Row(line.trim.split(",")(0).toDouble,line.trim.split(",")(1).toString))
    val df=spark.createDataFrame(rowRdd,types)
    df.show()

    spark.stop()

  }

}
