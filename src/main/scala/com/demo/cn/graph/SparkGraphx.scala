package com.demo.cn.graph

import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.{Edge, Graph, _}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD

object SparkGraphx {

  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("SparkGraphx")
      .getOrCreate()
    val sc = spark.sparkContext
//    sc.getConf.registerKryoClasses(Array(Class))

    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 23)),
      (7L, ("Jim", 43)),
      (8L, ("Terry", 21)),
      (9L, ("Terry", 21)),
      (10L, ("Terry", 21))

    )
    val edgeArray = Array(
      Edge(2L, 1L, 1),
      Edge(2L, 3L, 1),
      Edge(3L, 2L, 1),
      Edge(4L, 1L, 1),

      Edge(5L, 6L, 1),
      Edge(7L, 6L, 1),
      Edge(8L,7L,1),
      Edge(8L,5L,1),

      Edge(9L,10L,1)

    )

    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

    println("-----------connectedComponents-----------------")
    //连通图计算
    graph.connectedComponents().vertices.map(x=>{(x._2,x._1)}).groupByKey().foreach(println)

    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }
    println("the max outDegrees:"+graph.outDegrees.reduce(max))

    graph.collectNeighbors(EdgeDirection.Out).foreach(x=>print(x._1,x._2.mkString(",")))

    println(s"===========================")

    // (1)
    // display the names of the users that are at least 30 years old
    graph.vertices.filter { case (id, (vname, age)) => age > 30 }.collect.foreach {
      case (id, (sname, age)) => println(s"$sname is $age")
    }

    // (2)
    // Use the graph.triplets view to display who likes who

    graph.triplets.collect().foreach(x=>{println(s"${x.srcAttr._1} likes ${x.dstAttr._1}")})

    val inDegrees: VertexRDD[Int] = graph.inDegrees

    inDegrees.foreach(x=>{println(s"${x._1} inDegree is ${x._2}")})

    val subgraph=graph.subgraph(vpred =(id,attr)=>attr._2>30)
    subgraph.edges.foreach(x=>{println(s"the edge value ${x.srcId},${x.dstId},${x.attr}")})

    val pagrah=graph.pageRank(0.01).cache()
    val titleAndPrGraph = graph.outerJoinVertices(pagrah.vertices) {
      (v, title, rank) => (rank.getOrElse(0.0), title)
    }
    titleAndPrGraph.edges.foreach(x=>println("pageRank",x.srcId,x.dstId,x.attr))

    // Define a class to more clearly model the user property
    case class User(name: String, age: Int, inDeg: Int, outDeg: Int)
    // Create a user Graph
    val initialUserGraph: Graph[User, Int] = graph.mapVertices{ case (id, (name, age)) => User(name, age, 0, 0) }

    // Fill in the degree information
    val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
      case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
    }.outerJoinVertices(initialUserGraph.outDegrees) {
      case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg, outDegOpt.getOrElse(0))
    }


    // (3)
    // we restrict our graph to the users that are 30 or older.
    val olderGraph = userGraph.subgraph(vpred = (id, user) => user.age >= 30)
    // compute the connected components
    val cc = olderGraph.connectedComponents
    // display the component id of each user:
    olderGraph.vertices.leftJoin(cc.vertices) {
      case (id, user, comp) => s"${user.name} is in component ${comp.get}"
    }.collect.foreach{ case (id, str) => println(str) }


    spark.stop()
  }
}
