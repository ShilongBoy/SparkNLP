package com.demo.cn

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

object LRTest {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder().appName("LRTest").master("local[*]").getOrCreate()
    val sc=spark.sparkContext
    //隐式转换
    val colArrayName= Array("affairs", "gender", "age", "yearsmarried", "children", "religiousness", "education", "occupation", "rating")

    import spark.implicits._
    val dataDF = sc.textFile("file:///D:\\lrTest.txt")
      .map(x=>{x.split(" ,")
        .map(x=>{x.trim})
        .mkString(",")})
      .map(x=>{
       val values=x.split(",")
        values.length match {
          case 9=>lrdata(values(0).toDouble,values(1),values(2).toDouble,values(3).toDouble,values(4),values(5).toDouble,values(6).toDouble,values(7).toDouble,values(8).toDouble)
          case _=>null
        }
      }).toDF(colArrayName:_*)

    val colFeaturesName= Array("gender", "age", "yearsmarried", "children", "religiousness", "education", "occupation", "rating")
   //创建临时表
    dataDF.createOrReplaceTempView("df")
    val sql=
      s"""
        |select case when affairs>1 then 1 else 0 end as affairs,
        |case when gender='female' then 0 else 1 end as gender,
        |age,yearsmarried,
        |case when children='yes' then 1 else 0 end as children,
        |religiousness,education,occupation,rating
        |from df
      """.stripMargin

    val sqlDF = spark.sql(sql)

    val vecDF=new VectorAssembler().setInputCols(colFeaturesName).setOutputCol("features").transform(sqlDF)
    val Array(trainngData,testData)=vecDF.randomSplit(Array(0.9,0.1))

    trainngData.show()
    //建立model
    val model=new LogisticRegression().setLabelCol("affairs").setFeaturesCol("features").fit(trainngData)
    //截距，回归系统，此系数为每个特征的权重
    println(s"Coefficients: ${model.coefficients.toArray.mkString(",")} Intercept: ${model.intercept}")

    //测试环境
    model.transform(testData).show

    model.write.overwrite.save("D:\\tmp\\LRTest")
    spark.stop()

  }

  case class lrdata(affairs:Double, gender:String,age: Double, yearsmarried:Double, children:String, religiousness:Double, education:Double,occupation: Double, rating:Double)

}
