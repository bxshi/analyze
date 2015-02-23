package edu.nd.dsg.bshi.test

import edu.nd.dsg.bshi.PersonalizedPageRank
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite, ShouldMatchers}

class PersonalizedPageRankTest extends FunSuite with BeforeAndAfter with ShouldMatchers {
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

  test("Original PR in Spark") {
    val resGraph = testGraph.pageRank(0.001)
    resGraph.vertices.map(_._2).reduce(_+_) should be (0.705 +- 0.001)
  }

  test("PPR changed from runUntilConvergence") {
    val resGraph = PersonalizedPageRank.runUntilConvergence(testGraph, 1l, 0.0001)
    resGraph.vertices.foreach(println)
    resGraph.vertices.map(_._2).reduce(_+_) should be (1.85 +- 0.001)
  }

  test("PPR changed from run") {
    PersonalizedPageRank.run(testGraph, 1l, 10).vertices.map(_._2).reduce(_+_) should be (1.85 +- 0.001)
  }

  test("PPR with reversed pages") {
    val reversedGraph = Graph(testGraph.vertices, testGraph.edges.reverse)
    reversedGraph.edges.foreach(println)
    val resGraph = PersonalizedPageRank.runUntilConvergence(reversedGraph, 2l, 0.0001)
    resGraph.vertices.foreach(println)
    resGraph.vertices.map(_._2).reduce(_+_) should be (1.85 +- 0.001)
  }

  after {
    sc.stop()
  }

}
