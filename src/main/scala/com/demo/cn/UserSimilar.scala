package com.demo.cn

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.DenseVector
object UserSimilar {

  //1.设置环境变量
  val conf=new SparkConf().setMaster("local").setAppName("CollaborativeFilteringSpark")
  //2.实例化环境
  val sc=new SparkContext(conf)
  //3.设置用户
  val users=sc.parallelize(Array("aaa","bbb","ccc","ddd","eee"))
  //4.设置电影名
  sc.parallelize(Array("smzdm","ylxb","znb","nhsc","fcwr"))
  //5.使用一个source嵌套map作为姓名电影名和分值的存储
  var source=Map[String,Map[String,Int]]()
  //6.设置一个用以存放电影分的map
  val filmSource =Map[String,Int]()
  //7.设置电影评分
  def getSource():Map[String,Map[String,Int]]={
    val user1FilmSource=Map("smzdm"->2,"ylxb"->3,"znb"->1,"nhsc"->0,"fcwr"->1)
    val user2FilmSource=Map("smzdm"->1,"ylxb"->2,"znb"->2,"nhsc"->1,"fcwr"->4)
    val user3FilmSource=Map("smzdm"->2,"ylxb"->1,"znb"->0,"nhsc"->1,"fcwr"->4)
    val user4FilmSource=Map("smzdm"->3,"ylxb"->2,"znb"->0,"nhsc"->5,"fcwr"->3)
    val user5FilmSource=Map("smzdm"->5,"ylxb"->3,"znb"->1,"nhsc"->1,"fcwr"->2)
    //存储人的名字
    source += ("aaa" -> user1FilmSource)
    //存储人的名字
    source += ("bbb" -> user2FilmSource)
    //存储人的名字
    source += ("ccc" -> user3FilmSource)
    //存储人的名字
    source += ("ddd" -> user4FilmSource)
    //存储人的名字
    source += ("eee" -> user5FilmSource)
    //返回嵌套的map
    source
  }
  //采用余弦相似度两两计算分值
  def getCollaborateSource(user1:String,user2:String):Double={
    //获得第一个用户的评分
    val user1FilmSource =source.get(user1).get.values.toVector

    //获得第二个用户的评分
    val user2FileSource=source.get(user2).get.values.toVector
    println(s"user1FilmSource:${user1FilmSource},user2FileSource:${user2FileSource}")
    //对公示分子部分进行计算
    val member=user1FilmSource.zip(user2FileSource).map(d => d._1 *d._2).reduce(_+_).toDouble
    //求解分母的第一个变量
    val temp1=math.sqrt(user1FilmSource.map(num=>{math.pow(num,2)}).reduce(_+_))
    //求解分母第二个变量
    val temp2=math.sqrt(user2FileSource.map(num=>{math.pow(num,2)}).reduce(_+_))
    //求出分母
    val denominator=temp1*temp2
    //求出分式的值
    member/denominator
  }
  def main(args: Array[String]) {
    //初始化分数
    getSource()
    //设定目标对象
    val name="bbb"
    //进行迭代计算
    users.foreach(user=>{
      println(name+" 相对于"+user+"的相似性分数是："+getCollaborateSource(name,user))
    })
  }
}
