package com.demo.cn

import org.apache.spark.sql.SparkSession

object TestObj {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("SparkGraphx")
      .getOrCreate()
    val sc = spark.sparkContext
    val data = Seq(("a",3),("b",4),("a",1) )

    sc.parallelize(data).reduceByKey(_+_,10).foreach(println)

    val datas = List((1,4),(4,8),(0,4),(12,8))
    val rdd=sc.parallelize(datas)
    val rddInt=sc.makeRDD("1")
    println("rddInt:"+rddInt.first())

    System.out.println(
      """
        |Usage: RecoverableNetworkWordCount <hostname> <port> <checkpoint-directory>
        |     <output-file>. <hostname> and <port> describe the TCP server that Spark
        |     Streaming would connect to receive data. <checkpoint-directory> directory to
        |     HDFS-compatible file system which checkpoint data <output-file> file to which the
        |     word counts will be appended
        |
        |In local mode, <master> should be 'local[n]' with n > 1
        |Both <checkpoint-directory> and <output-file> must be absolute paths
      """.stripMargin
    )
//    rdd.mapValues(x=>{x+1})
  }

}
