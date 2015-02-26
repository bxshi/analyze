package edu.nd.dsg.bshi

import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * Implement Forward-Backward PPR here
 * Run FPPR first, get topK and log their values as (s_1, s_2, ..., s_k),
 * then run BPPR and log the values accordingly (s'_1, s'_2, ..., s'_k).
 * Calculate final score of topK as (\lambda * s_1/(s) + (1-\lambda)*s'_1/(s'), ...)
 * s and s' will be the initial score of source or the total score of entire graph in FPPR and BPPR
 */
object ForwardBackwardPPRRanking extends OutputWriter[String]{

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
  // Keys that we will write
  val stringKeys = Seq("title", "fppr_score", "fppr_rank", "bppr_score", "bppr_rank")
  var titleMap :Map[VertexId, String] = null
  /**
   * Load graph and save to graph variable
   * @param args All the needed variables
   */
  def Loader(args: Array[String]): Unit = {
    //TODO: Implement graph loader
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

    println("title map loaded")

    val conf = new SparkConf()
      .setMaster("local[" + nCores.toString + "]")
      .setAppName("Citation PageRank")
      .set("spark.executor.memory", "1g")
      .set("spark.driver.memory", "1g")
      .set("spark.driver.maxResultSize", "1g")
    val sc = new SparkContext(conf)

    // Input file should be space separated e.g. "src dst", one edge per line
    val file = sc.textFile(filePath).map(x => x.split(" ").map(_.toInt))

    vertices = sc.parallelize(file.map(_.toSet).reduce(_ ++ _).toSeq.map(x => (x.toLong, 0.0)))

    edges = sc.textFile(filePath).map(x => {
      val endPoints = x.split(" ").map(_.toInt)
      Edge(endPoints(0), endPoints(1), true)
    })

    graph = Graph(vertices, edges)

    Seq("","Graph statistics:",
      "--Vertices: " + graph.numVertices,
      "--Edges:    " + graph.numEdges).foreach(println)

    if (graph.vertices.filter(_._1 == queryId).count() == 0) {
      println("QueryId ", queryId, " does not exist!")
      sc.stop()
      return
    }
  }

  //if (arg.filter(_ == elem._1).length>0) 1/arg.length.toDouble else 0.0

  def SetInitialP(arg: Array[Long]): Unit ={
    val weighted_vertices: RDD[(VertexId, Double)] = vertices.map(elem => (elem._1, if (arg.filter(_ == elem._1).length>0) 1/arg.length.toDouble else 0.0))
    graph = Graph(weighted_vertices, edges)
  }
/**
* Run FBPPR
*/
  def run(): Unit = {
    var Initial_node = Array[Long](queryId)
    SetInitialP(Initial_node)

    val resGraph = PersonalizedPageRank.runWithInitialScoreUntilConvergence(graph, queryId, 0.00001,alpha)
    val fpprRankTopK = DataExtractor.extractTopKFromPageRank(resGraph,titleMap,50)
    fpprRankTopK.foreach(elem =>{
      if (!finalResult.contains(elem._1)) {
        finalResult(elem._1) = new mutable.HashMap[String, String]()
        finalResult(elem._1)("title")=elem._2.toString
        finalResult(elem._1)("fppr_score")=elem._4.toString
        finalResult(elem._1)("fppr_rank")=elem._3.toString
      }
      else {
        finalResult(elem._1)("title") = elem._2.toString
        finalResult(elem._1)("fppr_score") = elem._4.toString
        finalResult(elem._1)("fppr_rank") = elem._3.toString
      }
    })


    //TODO: Run F-PPR, get topK result

    //TODO: Run B-PPR on all topK, get result

    //TODO: Combine them together, save to finalResult

    //TODO: Call writeResult to write results
    writeResult(outputPath, finalResult, stringKeys)
    println("Finished!")
  }

}
