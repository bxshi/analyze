package edu.nd.dsg.bshi

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable

trait ArgLoader {

  var alpha = 0.15
  var queryId = 0l
  var filePath = ""
  var maxIter = 0
  var topK = 20
  var titlePath = ""
  var outputPath = ""
  var nCores = 4

  var vertices: RDD[(VertexId, Double)] = null
  var edges: RDD[Edge[Boolean]] = null
  var graph: Graph[Double,Boolean]=null  // original graph
  // final result {vid:{key1:val1, key2:val2, ...}}
  val finalResult = mutable.HashMap[VertexId, mutable.HashMap[String, String]]()
  var titleMap :Map[VertexId, String] = null


  def argLoader(args: Array[String]): Unit = {
    alpha = args(0).toDouble
    queryId = args(1).toLong
    maxIter = args(2).toInt
    topK = args(3).toInt
    nCores = args(4).toInt
    filePath = args(5)
    titlePath = args(6)
    outputPath = args(7)
    // Load article_list

    titleMap = scala.io.Source.fromFile(titlePath).getLines().map(x => {
      val tmp = x.split("\",\"").toList
      Map[VertexId, String]((tmp(0).replace("\"", "").toLong, tmp(1).replaceAll("\\p{P}", " ")))
    }).reduce(_ ++ _)
  }

}
