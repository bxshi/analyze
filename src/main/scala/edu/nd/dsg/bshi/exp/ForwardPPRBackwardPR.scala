package edu.nd.dsg.bshi.exp

import edu.nd.dsg.bshi.lib._
import org.apache.spark.graphx._

import scala.collection.mutable

/**
 * 1) Do PPR on a query point first, and then keep all the scores on each node,
 * do PR instead.
 *
 * 2) Do this several times and see if this can converge(degenerate) to PR
 *
 */
object ForwardPPRBackwardPR extends ExperimentTemplate[Double] with OutputWriter[String] {

  val stringKeys = Seq("pr_converge_rank", "pr_converge_score",
    "ppr_converge_rank", "ppr_converge_score",
    "bpr_converge_rank", "bpr_converge_score",
    "bpr_1_rank", "bpr_1_score",
    "bpr_2_rank", "bpr_2_score",
    "bpr_3_rank", "bpr_3_score",
    "bpr_4_rank", "bpr_4_score",
    "bpr_5_rank", "bpr_5_score")

  val delta = 0.001

  def load(args: Array[String]): Unit = {

    argLoader(args)

    val sc = createSparkInstance()

    if (!config.titlePath.isEmpty) {
      titleMap = sc.textFile(config.titlePath).map(x => {
        val tmp = x.split("\",\"").toList
        Map[VertexId, String]((tmp(0).replace("\"", "").toLong, tmp(1).replaceAll("\\p{P}", " ")))
      }).reduce(_ ++ _)
    }

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

  def run(): Unit = {
    // First get a converged PageRank result
    val prGraph = PageRank.runUntilConvergence(graph, tol = delta, resetProb = config.alpha)

    println("pr done")

    // Log result
    val prRes = DataExtractor.extractTopKFromPageRank(prGraph, titleMap, prGraph.numVertices.toInt)

    prRes.foreach(elem => {
      if (!finalResult.contains(elem._1)) {
        finalResult(elem._1) = new mutable.HashMap[String, String]()

      }
      finalResult(elem._1)("pr_converge_score") = elem._4.toString
      finalResult(elem._1)("pr_converge_rank") = elem._3.toString
    })

    println("pr stored")

    // Run PPR first
    val fpprGraph = PersonalizedPageRank.runUntilConvergence(graph, source = config.queryId,
      tol = delta, resetProb = config.alpha)
    println("fppr done")
    // Check difference between PR and PPR

    val pprRes = DataExtractor.extractTopKFromPageRank(fpprGraph, titleMap, fpprGraph.numVertices.toInt)
    pprRes.foreach(elem => {
      finalResult(elem._1)("ppr_converge_score") = elem._4.toString
      finalResult(elem._1)("ppr_converge_rank") = elem._3.toString
    })

    println("fppr stored")
    // Run backward PR until converge
//
//    val bprGraph = PageRank.runWithInitialScoreUntilConvergence(fpprGraph.reverse, tol = delta, resetProb = alpha)
//    println("bpr done")
//
//    val bprRes = DataExtractor.extractTopKFromPageRank(bprGraph, titleMap, bprGraph.numVertices.toInt)
//    bprRes.foreach(elem => {
//      finalResult(elem._1)("bpr_converge_score") = elem._4.toString
//      finalResult(elem._1)("bpr_converge_rank") = elem._3.toString
//    })

//    println("bpr stored")

    for(iterNum <- 1 to 15) {
      val bprIterGraph = PageRank.runWithInitialScore(fpprGraph.reverse, iterNum, config.alpha)
      DataExtractor.extractTopKFromPageRank(bprIterGraph, titleMap, bprIterGraph.numVertices.toInt)
        .foreach(elem => {
        finalResult(elem._1)("bpr_"+iterNum.toString+"_score") = elem._4.toString
        finalResult(elem._1)("bpr_"+iterNum.toString+"_rank") = elem._3.toString
      })
      println("bpr"+iterNum.toString+" stored")
    }

    writeResult(config.outPath, finalResult)

  }

}
