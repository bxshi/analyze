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
object ForwardBackwardPPRRanking extends OutputWriter[String] with ArgLoader{

  // Keys that we will write
  val stringKeys = Seq("title", "fppr_score", "fppr_rank", "bppr_score", "bppr_rank")

  /**
   * Load graph and save to graph variable
   * @param args All the needed variables
   */
  def Loader(args: Array[String]): Unit = {

    argLoader(args) // Load all common arguments

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

  def SetInitialP(arg: Array[Long]): RDD[(VertexId, Double)] ={
    vertices.map(elem => (elem._1, if (arg.filter(_ == elem._1).length>0) 1/arg.length.toDouble else 0.0))
  }
/**
* Run FBPPR
*/
  def run(): Unit = {
    var Initial_node = Array[Long](queryId)
    val newgraph = Graph(SetInitialP(Initial_node), edges)

    val resGraph = PersonalizedPageRank.runWithInitialScore(newgraph, queryId, maxIter ,alpha)
    val fpprRankTopK = DataExtractor.extractTopKFromPageRank(resGraph,titleMap,topK)

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

    fpprRankTopK.foreach(elem => {
      Initial_node = Array[Long](elem._1)
      val invgraph = Graph(SetInitialP(Initial_node), graph.edges.reverse)
      val resGraph = PersonalizedPageRank.runWithInitialScore(invgraph, elem._1, maxIter, alpha)
      val ans = DataExtractor.extractNodeFromPageRank(resGraph, titleMap, queryId)
      finalResult(elem._1)("bppr_score") = ans.head._4.toString()
      finalResult(elem._1)("bppr_rank") = ans.head._3.toString()
    })

    writeResult(outputPath, finalResult, stringKeys)
    println("Finished!")
  }

}
