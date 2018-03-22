package com.demo.cn

import java.io.PrintWriter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.functions._
import scala.tools.scalap.scalax.util.StringUtil

object SparkNLP {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkSessionZipsExample")
      .master("local[*]")
      .getOrCreate()

    //spark.conf.set("spark.sql.shuffle.partitions", 6)
    //spark.conf.set("spark.executor.memory", "2g")

    val configMap = spark.conf.getAll

    val langPercentDF = spark.createDataFrame(
      Seq(("0", "fxlj|2017-11-20 00:00:01|88888|797169|oPm98wKZ0YGbXgh4UgF0Gw1cCAM8-1511107201|1|share")
        , ("1", "fxlj|2017-11-19 23:59:57|15001|7019665|InviteRewardDialog-1511107197|share")
        , ("2", "fxlj|2017-11-20 00:00:00|20001|579299|oaGhewAzG0rhz3rXuryp1vygKZtA-1511107200|1|confirm")
        , ("3", "fxlj|2017-11-20 00:00:00|25001|9487996|o7LG5v9iYmVbB8w7apvnMq0QCBV0-1511107198|1|cancel")
        , ("4", "fxlj|2017-11-19 23:59:59|13001|11956827|oefDgvrxEeV5ZpBEpc6wLCMsN5tk-1511107198|0|cancel")
        , ("5", "fxlj|2017-11-19 23:59:59|13001|11956827|oefDgvrxEeV5ZpBEpc6wLCMsN5tk-1511107198|0|cancel|error")

      ))

    val lpDF = langPercentDF.withColumnRenamed("_1", "id").withColumnRenamed("_2", "message")

    val tmpDF = lpDF.addAlwaysColumn.appendDefaultVColumn(Map("bigdata" -> "hot"))
    tmpDF
    tmpDF.foreach(x => {
      val message = x.getAs[String]("message")
      println(message)
    })

    import spark.implicits._
    val resultDF = tmpDF.map(x => {
      val message = x.getAs[String]("message")
      val length = message.split("\\|").length
      length match {
        case 6 => {
          val str: Seq[String] = message.split("\\|")
          Message(str(0), str(1), str(2), str(3), str(4), "", str(5))
        }
        case 7 => {
          val str: Seq[String] = message.split("\\|")
          Message(str(0), str(1), str(2), str(3), str(4), str(5), str(6))
        }

        case _=>{
          Message("","","","","","","")
        }

      }
    })

    resultDF.filter(x=>filterFunction(x.flag)).show()

    def filterFunction(str:String):Boolean={
      if("".equals(str)) return false
      else return  true
    }



  }
}
case class Message(name:String,time:String,id:String,idType:String,macid:String,flag:String,action:String)
