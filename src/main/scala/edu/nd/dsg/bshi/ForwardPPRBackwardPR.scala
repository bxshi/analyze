package edu.nd.dsg.bshi

import org.apache.spark.graphx.{Graph, Edge}

/**
 * 1) Do PPR on a query point first, and then keep all the scores on each node,
 * do PR instead.
 *
 * 2) Do this several times and see if this can converge(degenerate) to PR
 *
 */
object ForwardPPRBackwardPR extends ExperimentTemplate with OutputWriter[String] {

  val stringKeys = Seq("title", "fppr_score", "fppr_rank", "bpr_score", "bpr_rank")

  val delta = 0.001

  def load(args: Array[String]): Unit = {

    argLoader(args)

    val sc = createSparkInstance()

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

  def run(): Unit = {
    // First get a converged PageRank result
    val prGraph = PageRank.runUntilConvergence(graph, tol = delta, resetProb = alpha)

    println("pr done")
    // Run PPR first
    val fpprGraph = PersonalizedPageRank.runUntilConvergence(graph, source = queryId,
      tol = delta, resetProb = alpha)
    println("fppr done")
    // Check difference between PR and PPR

    DataExtractor.extractTopKFromPageRank(fpprGraph, titleMap, topK).foreach(println)

    // Run PR w/ score
    val bprGraph = PageRank.runWithInitialScoreUntilConvergence(fpprGraph.reverse, tol = delta, resetProb = alpha)
    println("bpr done")
    // Check difference between PR and PPR

    // Output topK for test purpose

    DataExtractor.extractTopKFromPageRank(bprGraph, titleMap, topK).foreach(println)

  }

}
