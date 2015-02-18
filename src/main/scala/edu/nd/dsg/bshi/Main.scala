package edu.nd.dsg.bshi

import java.io.{PrintWriter, File}

import org.apache.spark.rdd.{RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import scala.collection.mutable

object Main {

  var alpha = 0.15
  var queryId = 0l
  var filePath = ""
  var maxIter = 0
  var topK = 20
  var titlePath = ""
  var outputPath = ""

  def extractPageRank(graph: Graph[(Double, Int), Double], titleMap: Map[VertexId, String], top: Int = topK): Seq[(VertexId, String, Int, Double)] = {
    val tmpRes = graph.vertices.map(x => (x._2._1, x._1)).top(top).map(y => (y._2, y._1)).toSeq
    tmpRes.zip(tmpRes.indices).map(x => (x._1._1, titleMap.getOrElse(x._1._1, ""), x._2, x._1._2))
  }

  def extractTargetPage(graph: Graph[(Double, Int), Double], titleMap: Map[VertexId, String], testId: VertexId): (VertexId, String, Int, Double) = {
    val collectedVertices = graph.vertices.sortBy(_._2._1, ascending = false).collect()
    val tmpRes = collectedVertices.filter(_._1 == queryId).head

    (testId, titleMap.getOrElse(testId, ""), collectedVertices.indexOf(tmpRes), tmpRes._2._1)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 7) {
      println("usage: alpha queryId maxIter topK edgeFilePath titleFilePath outputPath")
      return
    } else {
      alpha = args(0).toDouble
      queryId = args(1).toLong
      maxIter = args(2).toInt
      topK = args(3).toInt
      filePath = args(4)
      titlePath = args(5)
      outputPath = args(6)
    }

    println(alpha, queryId, maxIter, topK, filePath, titlePath)

    // Load article_list

    val titleMap = scala.io.Source.fromFile(titlePath).getLines().map(x => {
      val tmp = x.split("\",\"").toList
      Map[VertexId, String]((tmp(0).replace("\"","").toLong, tmp(1).replace("\"","")))
    }).reduce(_++_)

    println("title map loaded")

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

    // For final output
    val finalResult = mutable.HashMap[VertexId, mutable.HashMap[String, String]]()


    // pprank w/ normalization
    val pprRank = CitPageRank.pageRank(graph, queryId, maxIter, alpha)
    val pprRankTopK = extractPageRank(pprRank, titleMap)

    val pprNoNormRank = CitPageRank.pageRank(graph, queryId, maxIter, alpha, false)
    val pprNoNormRankTopK = extractPageRank(pprNoNormRank, titleMap)

    // Log prRank
    pprRankTopK.foreach( elem => {
      if (!finalResult.contains(elem._1)) {
        finalResult(elem._1) = new mutable.HashMap[String, String]()
        finalResult(elem._1)("source") = queryId.toString //Id of source
        finalResult(elem._1)("title") = elem._2 // Title of result
      }
      finalResult(elem._1)("pprank") = elem._3.toString // PPR rank with indeg normalization
      finalResult(elem._1)("pprscore") = elem._4.toString // PPR score with indeg normalization
    })

    pprNoNormRankTopK.foreach( elem => {
      if (!finalResult.contains(elem._1)) {
        finalResult(elem._1) = new mutable.HashMap[String, String]()
        finalResult(elem._1)("source") = queryId.toString
        finalResult(elem._1)("title") = elem._2
      }
      finalResult(elem._1)("pprank_nonorm") = elem._3.toString
      finalResult(elem._1)("pprscore_nonorm") = elem._4.toString
    })


    // TODO: Reduce graph size by year/nodes that related to query point(back-traverse)
    for(testId <- pprRankTopK.map(_._1)) { // Do reversed PPR on results
      println("test testId",testId)
      val tmpRank = CitPageRank.revPageRank(graph, testId, maxIter, alpha)
      val revRank = extractTargetPage(tmpRank, titleMap, testId)

      finalResult(testId)("rpprank") = revRank._3.toString
      finalResult(testId)("rpprscore") = revRank._4.toString
    }

    for(testId <- pprNoNormRankTopK.map(_._1)) {
      println("test testId",testId)
      val tmpRank = CitPageRank.revPageRank(graph, testId, maxIter, alpha)
      val revRank = extractTargetPage(tmpRank, titleMap, testId)

      finalResult(testId)("rpprank_nonorm") = revRank._3.toString
      finalResult(testId)("rpprscore_nonorm") = revRank._4.toString
    }

    sc.stop()

    val writer = new PrintWriter(new File(outputPath))
    val csvHeader = Seq("node", "source", "title", "pprank", "rpprank", "pprank_nonorm", "rpprank_nonorm", "rpprscore", "pprscore", "pprscore_nonorm", "rpprscore_nonorm")
    writer.write(csvHeader.reduce(_+","+_)+"\n")

    finalResult.map(elem => {
      val tuple = Seq(elem._1.toString, queryId.toString)
      val tupleRest = csvHeader.slice(2, csvHeader.size).map(x => elem._2.getOrElse(x, "NA").toString)
      writer.write((tuple ++ tupleRest).reduce(_+","+_)+"\n")
    })

    writer.close()
  }
}
