package edu.nd.dsg.bshi.test

import edu.nd.dsg.bshi.DataExtractor
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{BeforeAndAfter, ShouldMatchers, FunSuite}

class DataExtractorTest extends FunSuite with ShouldMatchers with BeforeAndAfter {

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

    vertices = sc.parallelize(List((1l,1.0),(2l,10.0),(3l,3.0)))
    edges = sc.parallelize(List((1l,2l),(3l,2l)).map(x=>Edge(x._1,x._2,true)))

    testGraph = Graph(vertices, edges)
  }

  test("extractNodeFromPageRank") {
    val res = DataExtractor.extractNodeFromPageRank(testGraph, Map[VertexId, String](), 3l)
    res.length should be (1)
    res.head._1 should be (3l)
    res.head._2 should be ("")
    res.head._3 should be (1)
    res.head._4 should be (3.0)
  }

  after {
    sc.stop()
  }

}
