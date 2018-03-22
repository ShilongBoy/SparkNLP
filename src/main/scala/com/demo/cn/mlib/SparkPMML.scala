package com.demo.cn.mlib
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.util.MLUtils

object SparkPMML {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("clusters")
      .getOrCreate()

    val sc=spark.sparkContext

    val data = sc.textFile("file:///D:\\Kmeans.txt")
    val parseData = data.map(s=>Vectors.dense(s.split(" ").map(_.toDouble))).cache()

    //使用KMeans算法将数据分为两类
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parseData, numClusters, numIterations)
    val test=sc.makeRDD(Seq(("5.0 7.0 9.0"),("9.0 9.0 9.0")))
    val testingData=test.map(x=>Vectors.dense(x.split(" ").map(_.toDouble)))
    val result=clusters.predict(testingData)

    //clusters.save(sc,"D:\\tmp\\clusters")

    //测试数据输出
     result.foreach(println)
    //导出PMML
    println("PMML Model:\n" + clusters.toPMML)

    //将PMML格式的模型导出为string
    clusters.toPMML
    //将PMML格式的模型导出为本地文件
    clusters.toPMML("D:\\tmp\\k.xml")
    //将PMML导出为分部式文件系统
//    clusters.toPMML(sc,"/tmp/kmeans")
    //将PMML格式导出为输出流
//    clusters.toPMML(System.out)

  }

}
