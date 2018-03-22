package com.demo.cn.etl

import org.apache.spark.sql.SparkSession

object SparkSQLPPT {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder()
      .appName("SparkSQLPPT")
      .master("local[*]")
      //.config("hive.metastore.uris", "thrift://master:9083")
//      .enableHiveSupport()
      .getOrCreate()

//    spark.sql("select * from spark_sql_test0103.employee_hr").show()

    import spark.implicits._

    val df=spark.createDataFrame(Seq(
      people("1","sky",112),
      people("2","tim",882),
      people("3","jack",899),
      people("4","jack",1000)

    )).toDF()

    println("-------------------")
    df.repartition(10)
    println("-------------------")

    df.rdd.map(x=>{
      (x.getAs[String]("name"),x.getAs[Int]("money"))
    }).reduceByKey(_ + _)
        .foreachPartition(x=>{
          x.foreach(println)
        })


    //spark.sql("select p1.*,p2.* from people p1,people p2").show()



  }

  case class people(id:String,name:String,money:Int)

}
