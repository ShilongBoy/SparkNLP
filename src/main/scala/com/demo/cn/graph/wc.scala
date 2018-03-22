package com.demo.cn.graph

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object wc {

    val spark = SparkSession.builder()
                .master("local[*]")
                .appName("Word2Vec")
                .getOrCreate()

    def main(args: Array[String]): Unit = {
      import spark.implicits._
      val documentDF = spark.createDataFrame(Seq(
        ("1","北京市朝阳区春晓园北区7号楼".wordSplit()),
        ("2","北京市海淀区学院路20号院甲2号楼".wordSplit()),
        ("3","北京市西城区复兴门北大街4号楼6".wordSplit()),
        ("4","北京市西城区西交民巷64号院1号楼2".wordSplit()),
        ("5","北京市海淀区花园路10号院3号楼7".wordSplit())

      )).toDF("id","name")

      val bdDoucmentDF=spark.sparkContext.broadcast(documentDF)

      documentDF.show()

      //设置特征向量维数为5
      val word2Vec = new Word2Vec().setInputCol("name").setOutputCol("outName").setVectorSize(5).setMinCount(0)
      val word2Vec_model = word2Vec.fit(documentDF) //训练模型
      val result = word2Vec_model.transform(documentDF) //把文档转换成特征向量
      import spark.implicits._
      val tmpDF=result.map(x=>{
        val outName=x.getAs[DenseVector]("outName")
        val id=x.getAs[String]("id")
        (s"$id&$outName")
      }).toDF("outname")

      val rdd=tmpDF.selectExpr("outname").rdd.cartesian(tmpDF.selectExpr("outname").rdd)
      val resultDF=rdd.map(x=>{
        val str1=x._1.getAs[String](0)
        val str2=x._2.getAs[String](0)
        val value1=str1.split("&")(0)
        val v2=str1.split("&")(1).replace("[","").replace("]","").split(",").toSeq.map(x=>(x.toDouble))
        val value3=str2.split("&")(0)
        val v4=str2.split("&")(1).replace("[","").replace("]","").split(",").toSeq.map(x=>(x.toDouble))
        (value1,v2,value3,v4)
      }).toDF("v1","v2","v3","v4")

      val graphDF=resultDF.mapPartitions(x=>{
        x.map(x=>{
          val str1=x.getAs[Seq[Double]]("v2")
          val str2=x.getAs[Seq[Double]]("v4")
          val v1=x.getAs[String]("v1")
          val v2=x.getAs[String]("v3")
          val simV=(similarValue(str1,str2)).toDouble
          (v1,v2,str1,str2,simV)
        })
      }).toDF("v1","v2","str1","str2","simV")
//        .filter(x=>x.getAs[Double]("simV")>0.8)
      graphDF.show()
      //图顶点的RDD
      val vertexRDD=graphDF.mapPartitions(x=>{
        x.map(x=>{
          val soure=x.getAs[String]("v1").toLong
          val desc=x.getAs[String]("v2").toLong
          val height=x.getAs[Double]("simV")
          (soure,(desc,height))
        })
      }).rdd
      //图边的RDD
      val edgeRDD=graphDF.mapPartitions(x=>{
        x.map(x=>{
          val soure=x.getAs[String]("v1").toLong
          val desc=x.getAs[String]("v2").toLong
          Edge(soure,desc,1.0)
        })
      }).rdd
      val graph = Graph(vertexRDD, edgeRDD)
      //算出连通图
      graph.connectedComponents().vertices.map(x=>{(x._2,x._1)}).groupByKey().foreach(println)
      spark.stop()
    }

  def similarValue(str1:Seq[Double],str2:Seq[Double]):Double={
    val vec1=str1.toVector
    val vec2=str2.toVector
    val member=vec1.zip(vec2).map(d => d._1*d._2).reduce(_+_)
    //求解分母的第一个变量
    val temp1=math.sqrt(vec1.map(num=>{math.pow(num,2)}).reduce(_+_))
    //求解分母第二个变量
    val temp2=math.sqrt(vec2.map(num=>{math.pow(num,2)}).reduce(_+_))
    //求出分母
    val denominator=temp1*temp2
    //求出分式的值
    member/denominator

  }
  case class appinfo(id:String,vectorArray:Seq[Double])
}
