package edu.nd.dsg.bshi

import org.apache.spark.graphx._

import scala.reflect.ClassTag


object PageRank {
  /**
   * Run PageRank for a fixed number of iterations returning a graph
   * with vertex attributes containing the PageRank and edge
   * attributes the normalized edge weight.
   *
   * This method will lose some initial scores when
   * (1) -> (2) <- (3)
   *
   * Score of (2) is damping + ((1)+(3)) * 0.85
   *
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank,
   *              the value on each vertex will be used as initial score of that node
   * @param numIter the number of iterations of PageRank to run
   * @param resetProb the random reset probability (alpha)
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   *
   */
  def runWithInitialScore[ED: ClassTag](graph: Graph[Double, ED],
                        numIter: Int,
                        resetProb: Double = 0.15): Graph[Double, Double] =
  {
    // Initialize the PageRank graph with each edge attribute having
    // weight 1/outDegree and each vertex with attribute 1.0.
    var rankGraph: Graph[Double, Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => (vdata, deg.getOrElse(0)) }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr._2, TripletFields.Src )
      // Here we do not reset values
      .mapVertices((id, data) => data._1)

    var iteration = 0
    var prevRankGraph: Graph[Double, Double] = null
    while (iteration < numIter) {
      rankGraph.cache()

      // Compute the outgoing rank contributions of each vertex, perform local preaggregation, and
      // do the final aggregation at the receiving vertices. Requires a shuffle for aggregation.
      val rankUpdates = rankGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)

      // Apply the final rank updates to get the new ranks, using join to preserve ranks of vertices
      // that didn't receive a message. Requires a shuffle for broadcasting updated ranks to the
      // edge partitions.
      prevRankGraph = rankGraph
      rankGraph = rankGraph.joinVertices(rankUpdates) {
        (id, oldRank, msgSum) => resetProb + (1.0 - resetProb) * msgSum
      }.cache()

      rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      iteration += 1
    }

    rankGraph
  }

  /**
   * Run PageRank for a fixed number of iterations returning a graph
   * with vertex attributes containing the PageRank and edge
   * attributes the normalized edge weight.
   *
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank,
   *              the value on each vertex will be used as initial score of that node
   * @param numIter the number of iterations of PageRank to run
   * @param resetProb the random reset probability (alpha)
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   *
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[Double, ED],
                                        numIter: Int,
                                        resetProb: Double = 0.15): Graph[Double, Double] = {
    graph.staticPageRank(numIter, resetProb)
  }

  /**
   * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
   * PageRank and edge attributes containing the normalized edge weight.
   *
   * This method can preserve all initial scores, because for each iteration,
   * the score is increasing based on old score.
   *
   * Remind that this method will makes the score keep increasing.
   *
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank
   * @param tol the tolerance allowed at convergence (smaller => more accurate).
   * @param resetProb the random reset probability (alpha)
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   */
  def runWithInitialScoreUntilConvergence[ED: ClassTag](graph: Graph[Double, ED],
                                                      tol: Double,
                                                      resetProb: Double = 0.15): Graph[Double, Double] =
  {
    // Initialize the pagerankGraph with each edge attribute
    // having weight 1/outDegree and each vertex with attribute 1.0.
    val pagerankGraph: Graph[(Double, Double), Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) {
      (vid, vdata, deg) => (vdata, deg.getOrElse(0))
    }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr._2 )
      // Set the vertex attributes to (initalPR, delta = 0)
      .mapVertices( (id, attr) => (attr._1, 0.0) )
      .cache()

    // Define the three functions needed to implement PageRank in the GraphX
    // version of Pregel
    def vertexProgram(id: VertexId, attr: (Double, Double), msgSum: Double): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      if (msgSum == 0.0) {
        (oldPR, oldPR)
      } else {
        val newPR = oldPR + (1.0 - resetProb) * msgSum
        (newPR, newPR - oldPR)
      }
    }

    def sendMessage(edge: EdgeTriplet[(Double, Double), Double]) = {
      if (edge.srcAttr._2 > tol) {
        Iterator((edge.dstId, edge.srcAttr._2 * edge.attr))
      } else {
        Iterator.empty
      }
    }

    def messageCombiner(a: Double, b: Double): Double = a + b

    // The initial message received by all vertices in PageRank
    val initialMessage = 0.0

    // Execute a dynamic version of Pregel.
    Pregel(pagerankGraph, initialMessage, activeDirection = EdgeDirection.Out)(
      vertexProgram, sendMessage, messageCombiner)
      .mapVertices((vid, attr) => attr._1)
  } // end of deltaPageRank

  /**
   * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
   * PageRank and edge attributes containing the normalized edge weight.
   *
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank
   * @param tol the tolerance allowed at convergence (smaller => more accurate).
   * @param resetProb the random reset probability (alpha)
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   */
  def runUntilConvergence[VD: ClassTag, ED: ClassTag](graph: Graph[Double, ED],
                                                        tol: Double,
                                                        resetProb: Double = 0.15): Graph[Double, Double] = {
    graph.pageRank(tol, resetProb)
  }

}
