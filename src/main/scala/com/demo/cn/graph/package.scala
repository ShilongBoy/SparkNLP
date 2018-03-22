package com.demo.cn

import java.text.SimpleDateFormat
import java.util.Date

import com.hankcs.hanlp.HanLP
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._

package object graph {

  val simpleDataFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  private lazy val timeStampFormatter = simpleDataFormat.format()

  private lazy val usenessWs = Seq("号")

  private lazy val segments = HanLP
    .newSegment()
    .enableAllNamedEntityRecognize(true)

  implicit class AppendColumn(df:DataFrame) {

    def addAlwaysColumn: DataFrame = {
      df.withColumn("row_crt_ts", lit(simpleDataFormat.format(new Date())))
    }

    def appendDefaultVColumn(defaultMap: Map[String, Any]): DataFrame = {
      var tmpDF = df
      for ((k, v) <- defaultMap) {
        tmpDF = tmpDF.withColumn(k, lit(v))
      }
      tmpDF
    }

  }

  /**
    *
    * @param word
    */
  implicit class WordSplit(word: String) extends Serializable {

    def wordSplit(flag: Boolean = false): Seq[String] = {
      Option(word) match {
        case None => Seq.empty[String]
        case Some(s) => {
          val el = segments.seg(s.trim)
          //      CoreStopWordDictionary.apply(el)
          val result = if (el.isEmpty) Seq.empty[String]
          else {
            //取地理名词
            el.filter(_.nature.name() == "ns")
              .map(x => {
                x.word.trim.replaceAll(" ", "")
              }).filterNot(_.isEmpty).distinct
          }
          flag match {
            case false => result
            case true => result.map(_.replaceAll(usenessWs.mkString("[", " ", "]"), ""))
          }
        }
      }
    }
  }



}
