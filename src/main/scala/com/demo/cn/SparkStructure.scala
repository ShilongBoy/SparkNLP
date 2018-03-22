package com.demo.cn
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
object SparkStructure {

  def main(args: Array[String]): Unit = {



    val spark = SparkSession
      .builder
        .master("local[*]")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "10.1.69.11")
      .option("port", 9999)
      .load()

    val words = lines.as[String].flatMap(_.split(" "))

    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }

}
