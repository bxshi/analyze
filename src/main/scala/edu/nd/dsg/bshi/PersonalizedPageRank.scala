package edu.nd.dsg.bshi

import scala.reflect.ClassTag
import org.apache.spark.graphx._


object PersonalizedPageRank {

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], source: VertexId, maxIter: Int, resetProb: Double = 0.15): Graph[Double, Double] =
  {
    var rankGraph: Graph[Double, Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      // Set the weight on the edges based on the degree
      // The weight's distribution of a node, instead of the absolute value of edge weight matters
      .mapTriplets( e => 1.0 / e.srcAttr, TripletFields.Src )
      // Set the vertex attributes to the initial pagerank values
      .mapVertices( (id, attr) => {
      // At initialization stage, only source has weight
      if (id == source) 1.0 else 0.0
      // bind in-degree to each node
    })

    var iteration = 0
    var prevRankGraph: Graph[Double, Double] = null
    while (iteration < maxIter) {
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
        // Set restart probability to personalized version(all restart will direct to source node)
        //        (id, oldRank, msgSum) => ((if (id == startPoint) alpha * numVertices else 0 ) + (1.0 - alpha) * msgSum / (if (normalize) oldRank._2 else 1), oldRank._2)
        (id, oldRank, msgSum) => (if (id == source) resetProb  else 0.0 ) + (1.0 - resetProb) * msgSum
      }.cache()

      rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      if (iteration % 5 == 0) println(s"PageRank finished iteration $iteration.")
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      iteration += 1

    }

    rankGraph
  }

  def runUntilConvergence[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], source: VertexId, tol: Double, resetProb: Double = 0.15): Graph[Double, Double] =
  {
    val pprGraph: Graph[(Double, Double), Double] = graph
        .outerJoinVertices(graph.outDegrees) {
      (vid, vdata, deg) => deg.getOrElse(0)
    }.mapTriplets( e => 1.0 / e.srcAttr)
    .mapVertices( (id, attr) => (0.0, 0.0)).cache()

    def vertexProgram(id: VertexId, attr: (Double, Double), msgSum: Double): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      if(msgSum == 0.0 && oldPR == 0.0 && id == source) {
        (1.0, 1.0)
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

    val initialMessage = 0.0

    Pregel(pprGraph, initialMessage, activeDirection = EdgeDirection.Out)(
      vertexProgram, sendMessage, messageCombiner).mapVertices((vid, attr) => attr._1)
  }
}
