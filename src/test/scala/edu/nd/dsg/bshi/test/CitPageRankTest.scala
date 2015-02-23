package edu.nd.dsg.bshi.test

import edu.nd.dsg.bshi.CitPageRank
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

class CitPageRankTest extends FunSuite with BeforeAndAfter {

  var conf: SparkConf = null
  var sc: SparkContext = null
  val alpha = 0.15

  var vertices: RDD[(VertexId, Double)] = null
  var edges: RDD[Edge[Boolean]] = null
  var testGraph: Graph[Double, Boolean] = null

  before {
    conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("CitPageRankTest")
    sc = new SparkContext(conf)

    vertices = sc.parallelize(List((1l,0.0),(2l,0.0),(3l,0.0)))
    edges = sc.parallelize(List((1l,2l),(3l,2l)).map(x=>Edge(x._1,x._2,true)))

    testGraph = Graph(vertices, edges)
  }

  test("A normal Personalized PageRank with 10 iteration") {

    val queryId = 1
    val maxIter = 10

    val resGraph = CitPageRank.pageRank(testGraph, queryId, maxIter, alpha, false)
    val resMap = resGraph.vertices.map(x => (x._1, x._2._1)).collect().toMap
    assert(resMap.getOrElse(1, 0) == 1)
    assert(resMap.getOrElse(3, 0) == 0.0)
    assert(resMap.getOrElse(2, 0) == 0.85)
  }

  test("Personalized PageRank with in-degree normalization") {
    val queryId = 1
    val maxIter = 10

    val resGraph = CitPageRank.pageRank(testGraph, queryId, maxIter, alpha, true)
    val resMap = resGraph.vertices.map(x => (x._1, x._2._1)).collect().toMap
    assert(resMap.getOrElse(1, 0) == 1)
    assert(resMap.getOrElse(3, 0) == 0.0)
    assert(resMap.getOrElse(2, 0) == 0.85)
  }

  test("A reversed version of Personalized PageRank with one iteration") {

    val queryId = 2
    val maxIter = 1
    val resMap = CitPageRank.revPageRank(testGraph, queryId, maxIter, alpha, false)
      .vertices.map(x=>(x._1,x._2._1)).collect().toMap
    assert(resMap.getOrElse(1,0) == 1.275)
    assert(resMap.getOrElse(2,0) == 3.0)
    assert(resMap.getOrElse(3,0) == 1.275)
  }

  test("Reversed Personalized PageRank with in-degree normalization") {
    val queryId = 2
    val maxIter = 1
    val resMap = CitPageRank.revPageRank(testGraph, queryId, maxIter, alpha, true)
      .vertices.map(x=>(x._1,x._2._1)).collect().toMap
    assert(resMap.getOrElse(1,0) == 1.275)
    assert(resMap.getOrElse(2,0) == 3.0)
    assert(resMap.getOrElse(3,0) == 1.275)
  }

  test("More complicated reversed PPR") {
    val vertices: RDD[(VertexId, Double)] = sc.parallelize(List(1,2,3,4,5).map(x => (x.toLong, 0.0)))
    val edges: RDD[Edge[Boolean]] = sc.parallelize(List((1l,2l),(1l,5l),(1l,3l),(2l,4l),(5l,4l)).map(x=>Edge(x._1,x._2,true)))
    val g = Graph(vertices, edges)
    val queryId = 5
    val maxIter = 5
    val resMap = CitPageRank.revPageRank(g, queryId, maxIter, alpha, false)
      .vertices.map(x=>(x._1,x._2._1)).collect().toMap
    assert(resMap.getOrElse(1,0) == 0.6375)
    assert(resMap.getOrElse(2,0) == 0)
    assert(resMap.getOrElse(3,0) == 0)
    assert(resMap.getOrElse(4,0) == 0)
    assert(resMap.getOrElse(5,0) == 0.75)
  }

  after {
    sc.stop()
  }

}
