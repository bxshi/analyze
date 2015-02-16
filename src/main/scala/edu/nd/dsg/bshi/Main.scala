package edu.nd.dsg.bshi

import org.apache.spark.rdd.{RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

object Main {

  var alpha = 0.15
  var queryId = 0l
  var filePath = ""
  var maxIter = 0
  var topK = 20
  var titlePath = ""

  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      println("usage: alpha queryId maxIter topK edgeFilePath titleFilePath")
      return
    } else {
      alpha = args(0).toDouble
      queryId = args(1).toLong
      maxIter = args(2).toInt
      topK = args(3).toInt
      filePath = args(4)
      titlePath = args(5)
    }

    println(alpha, queryId, maxIter, topK, filePath, titlePath)

    val conf = new SparkConf()
      .setMaster("local[10]")
      .setAppName("Citation PageRank")
      .set("spark.executor.memory", "1g")
      .set("spark.driver.memory", "1g")
      .set("spark.driver.maxResultSize", "1g")
    val sc = new SparkContext(conf)

    // Input file should be space separated e.g. "src dst", one edge per line
    val file = sc.textFile(filePath).map(x => x.split(" ").map(_.toInt))

    val vertices: RDD[(VertexId, Double)] = sc.parallelize(file.map(_.toSet).reduce(_++_).toSeq.map(x => (x.toLong, 0.0)))

    val edges: RDD[Edge[Boolean]] = sc.textFile(filePath).map(x => {
      val endPoints = x.split(" ").map(_.toInt)
      Edge(endPoints(0), endPoints(1), true)
    })

    val graph = Graph(vertices, edges)

    val totalV = graph.numVertices
    val totalE = graph.numEdges

    val zeroDeg = graph.outDegrees.filter(_._2==0l).count()

    Seq("Graph statistics:",
      "--Vertices:"+totalV,
      "--Edges"+totalE,
      "--OutDeg==0:"+zeroDeg).foreach(println)

    if(graph.vertices.filter(_._1 == queryId).count() == 0) {
      println("QueryId ", queryId, " does not exist!")
      sc.stop()
      return
    }

    var rankGraph: Graph[(Double, Int), Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr, TripletFields.Src )
      // Set the vertex attributes to the initial pagerank values
      .mapVertices( (id, attr) => {
      // At initialization stage, only source has weight
      if (id == queryId) totalV.toDouble else 0.0
    } ).outerJoinVertices(graph.inDegrees) { (vid, vdata, inDeg) => (vdata, inDeg.getOrElse(0))}

    val prRank = CitPageRank.pageRank(graph, queryId, maxIter, alpha).vertices.map(x => (x._2._1, x._1)).top(topK).map(y => (y._2, y._1)).toSeq

    prRank.foreach(println)

    var rprRank: Seq[(Int, (Int, Double))] = Seq.empty

    // TODO: Early stop based on year
    for(testId <- prRank.map(_._1)) {
      println("test testId",testId)
      val tmpRank = CitPageRank.revPageRank(graph, testId, maxIter, alpha)
      val revRank = tmpRank.vertices.map(x => (x._2._1, x._1)).top(totalV.toInt).map(y => (y._2.toInt, y._1)).toSeq

      tmpRank.vertices.filter(_._1 == queryId).foreach(println)

      revRank.indices.foreach(ind => {
        if (revRank(ind)._1 == queryId) {
          rprRank = rprRank.+:((ind, (testId.toInt, revRank(ind)._2))) // (index, (testId, score for queryId))
          println("get It")
        }
      })

    }

    sc.stop()

    // Load article_list

    val titleMap = scala.io.Source.fromFile(titlePath).getLines().map(x => {
      val tmp = x.split("\",\"").toList
      Map[Int, String]((tmp(0).replace("\"","").toInt, tmp(1)))
    }).reduce(_++_)

    println("original PPR")
    prRank.indices.foreach(ind => println(ind, prRank(ind), titleMap.get(prRank(ind)._1.toInt)))

    println("reversed PPR")
    val finalRank = rprRank.sortBy(_._1)
    finalRank.indices.foreach(ind => println(ind, finalRank(ind), titleMap.get(finalRank(ind)._2._1)))

  }
}
