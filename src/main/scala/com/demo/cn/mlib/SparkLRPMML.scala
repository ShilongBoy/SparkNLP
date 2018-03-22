package com.demo.cn.mlib

import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS, LogisticRegressionWithSGD}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

object SparkLRPMML {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
             .builder()
             .master("local[*]")
             .appName("SparkLRPMML")
             .getOrCreate()
    val sc=spark.sparkContext
    val data=MLUtils.loadLibSVMFile(sc,"file:///D:\\lib_svm.txt")
    val splits=data.randomSplit(Array(0.7,0.3))
    val trainingData=splits(0)
    val testingData=splits(1)

    val model=new LogisticRegressionWithLBFGS().setNumClasses(2).run(trainingData)

    val predictionAndLabels = testingData.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val precision = metrics.precision
    println("Precision = " + precision)
//    println("Precision = " + precision)
    println("intercept："+model.intercept)


    model.toPMML("D:\\tmp\\lr.xml")
    println("numFeature："+model.numFeatures)

  }

}
