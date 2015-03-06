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
object ForwardBackwardPPRRanking extends ExperimentTemplate with OutputWriter[String] {

  // Keys that we will write
  val stringKeys = Seq("title", "fppr_score", "fppr_rank", "bppr_score", "bppr_rank","Num_of_rel")

  /**
   * Load graph and save to graph variable
   * @param args All the needed variables
   */
  def load(args: Array[String]): Unit = {

    argLoader(args) // Load all common arguments
    
    val sc = createSparkInstance()

    // Input file should be space separated e.g. "src dst", one edge per line
    val file = sc.textFile(config.filePath).filter(!_.contains("#"))
      .map(x => x.split("\\s").map(_.toInt))

    vertices = sc.parallelize(file.map(_.toSet).reduce(_ ++ _).toSeq.map(x => (x.toLong, 0.0)))

    edges = file.map(x => Edge(x(0), x(1), true))

    graph = Graph(vertices, edges)

    Seq("","Graph statistics:",
      "--Vertices: " + graph.numVertices,
      "--Edges:    " + graph.numEdges).foreach(println)

    if (graph.vertices.filter(_._1 == config.queryId).count() == 0) {
      println("QueryId ", config.queryId, " does not exist!")
      sc.stop()
      return
    }
  }

  def setInitialP(arg: Array[Long]): RDD[(VertexId, Double)] ={
    vertices.map(elem => (elem._1, if (arg.filter(_ == elem._1).length>0) 1/arg.length.toDouble else 0.0))
  }

  /**
  * Run FBPPR
  */
  def run(): Unit = {
    println(math.pow(1-config.alpha,config.c.toDouble))

    var Initial_node = Array[Long](config.queryId)
    val newgraph = Graph(setInitialP(Initial_node), edges)
    val resGraph = PersonalizedPageRank.runWithInitialScore(newgraph, config.queryId, config.maxIter, config.alpha)
    val fpprRankTopK = DataExtractor.extractTopKFromPageRank(resGraph,titleMap, config.topK)

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
      val invgraph = Graph(setInitialP(Initial_node), graph.edges.reverse)
      val resGraph = PersonalizedPageRank.runWithInitialScore(invgraph, elem._1, config.maxIter, config.alpha)
      val ans = DataExtractor.extractNodeFromPageRank(resGraph, titleMap, config.queryId)
      finalResult(elem._1)("bppr_score") = ans.head._4.toString
      finalResult(elem._1)("bppr_rank") = ans.head._3.toString
      finalResult(elem._1)("Num_of_rel") = resGraph.vertices.filter(
        x=> x._2 > math.pow(1-config.alpha, config.c.toDouble)).count().toString
      println(finalResult(elem._1)("Num_of_rel"))
    })

    writeResult(config.outPath, finalResult, stringKeys)
    println("Finished!")
  }

}
