package com.demo.cn.recommend

import org.apache.spark.sql.SparkSession

object SparkRecommend {

  def main(args: Array[String]): Unit = {
    val sparkSession=SparkSession.builder().appName("SparkRecommend")
      //.config("spark.sql.warehouse.dir", "/apps/hive/warehouse")
      //.master("local[*]")
      .enableHiveSupport()
      .getOrCreate()


    val app_info_hive=sparkSession.sql("select * from spark_sql_test0103.app_info_hive")

    app_info_hive.show()

    sparkSession.stop()

/*
    val moviesData=sparkSession.sql("select movieid,title,gener from move.tx_movies").rdd

    moviesData.map(x=>{
      val movieid=x.getAs[String]("movieid")
      val title=x.getAs[String]("title")
      (movieid,title)
    })

    val tagsData=sparkSession.sql("select movied,tags from move.tx_tags").rdd
*/


  }

}
