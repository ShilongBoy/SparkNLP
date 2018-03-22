package com.demo.cn.graph

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession
object IKWords {



  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Word2Vec").master("local[*]")
      .getOrCreate()

    val df=spark.createDataFrame(Seq(("1","湖北武汉市汉口北大道88号"),
      ("2","成都青羊区清江中路65号4栋3单元2楼2号"),
      ("3","地址是乱输入的")
    )).toDF("id","address")


    import spark.implicits._
    val tmpDf=df.map(r=>{
        val id=r.getAs[String]("id")
        val address=r.getAs[String]("address")
        val result=address.wordSplit()
        (id,address,result)
      }).filter(x=>{
      x._3!=""
    }).toDF("id","address","address_split")
    tmpDf.show()

    val w2Vec=new Word2Vec()
      .setInputCol("address_split")
      .setOutputCol("outAdress")
      .setVectorSize(3)
      .setMinCount(0)

    val model=w2Vec.fit(tmpDf)
    model.transform(tmpDf).foreach(x=>{
      x.getAs[String]("outAdress")
    })

    spark.stop()
  }

  case class Adress(id:String,adress: String,result:String)

}
