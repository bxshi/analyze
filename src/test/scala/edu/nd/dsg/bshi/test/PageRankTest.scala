package edu.nd.dsg.bshi.test

import edu.nd.dsg.bshi.lib.PageRank
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{BeforeAndAfter, ShouldMatchers, FunSuite}


class PageRankTest extends FunSuite with ShouldMatchers with BeforeAndAfter{
  var conf: SparkConf = null
  var sc: SparkContext = null
  val alpha = 0.15

  var vertices: RDD[(VertexId, Double)] = null
  var edges: RDD[Edge[Boolean]] = null
  var testGraph: Graph[Double, Boolean] = null

  before {
    conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("PersonalizedPageRankTest")
    sc = new SparkContext(conf)

    vertices = sc.parallelize(List((1l,0.0),(2l,0.0),(3l,0.0)))
    edges = sc.parallelize(List((1l,2l),(3l,2l)).map(x=>Edge(x._1,x._2,true)))

    testGraph = Graph(vertices, edges)
  }

  test("Initialize before PR program") {
    val g = testGraph.mapVertices((vid, vd) => 0.15)
    val resGraph = PageRank.runWithInitialScore(g, 10)
    resGraph.vertices.map(_._2).reduce(_+_) should be (0.705 +- 0.001)
  }

  test("Customized initial scores") {
    val g = Graph(sc.parallelize(List((1l,1.0),(2l,0.3),(3l,0.5))), testGraph.edges)
    val resGraph = PageRank.runWithInitialScore(g, 10)
    resGraph.vertices.map(_._2).reduce(_+_) should be (2.925 +- 0.001)
  }

  test("Initialize before PR-converge program") {
    val g = testGraph.mapVertices((vid, vd) => 0.15)
    val resGraph = PageRank.runWithInitialScoreUntilConvergence(g, 0.001)
    resGraph.vertices.map(_._2).reduce(_+_) should be (0.705 +- 0.001)
  }

  test("Customized initial scores with PR-converge") {
    val g = Graph(sc.parallelize(List((1l,1.0),(2l,0.3),(3l,0.5))), testGraph.edges)
    val resGraph = PageRank.runWithInitialScoreUntilConvergence(g, 0.001)
    resGraph.vertices.map(_._2).reduce(_+_) should be (3.075 +- 0.001)
  }

  test("Keep using PR-converge will increase the total weight on the graph") {
    val g = Graph(sc.parallelize(List((1l,1.0),(2l,0.3),(3l,0.5))), testGraph.edges)
    val resGraph = PageRank.runWithInitialScoreUntilConvergence(g, 0.001)
    resGraph.vertices.map(_._2).reduce(_+_) should be (3.075 +- 0.001)

    val resGraph2 = PageRank.runWithInitialScoreUntilConvergence(resGraph, 0.001)
    resGraph2.vertices.map(_._2).reduce(_+_) should be (4.35 +- 0.001)
  }

  after {
    sc.stop()
  }
}
