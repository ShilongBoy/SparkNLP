package com.demo.cn

import org.apache.spark.sql.SparkSession

object SparkJDBC {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Word2Vec").master("local[*]")
      .getOrCreate()


  }

}
