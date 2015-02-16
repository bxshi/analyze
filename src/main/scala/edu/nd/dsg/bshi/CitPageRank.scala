package edu.nd.dsg.bshi

import org.apache.spark.graphx._

import scala.reflect.ClassTag

object CitPageRank {
  def pageRank[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                           startPoint: VertexId,
                                           maxIter: Int,
                                           alpha: Double): Graph[(Double, Int), Double] = {

    val numVertices = graph.numVertices

    var rankGraph: Graph[(Double, Int), Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      // Set the weight on the edges based on the degree
      // The weight's distribution of a node, instead of the absolute value of edge weight matters
      .mapTriplets( e => 1.0 / e.srcAttr, TripletFields.Src )
      // Set the vertex attributes to the initial pagerank values
      .mapVertices( (id, attr) => {
      // At initialization stage, only source has weight
      if (id == startPoint) numVertices.toDouble else 0.0
      // bind in-degree to each node
    } ).outerJoinVertices(graph.inDegrees) { (vid, vdata, inDeg) => (vdata, inDeg.getOrElse(0))}

    var iteration = 0
    var prevRankGraph: Graph[(Double, Int), Double] = null
    while (iteration < maxIter) {
      rankGraph.cache()

      // Compute the outgoing rank contributions of each vertex, perform local preaggregation, and
      // do the final aggregation at the receiving vertices. Requires a shuffle for aggregation.
      val rankUpdates = rankGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr._1 * ctx.attr), _ + _, TripletFields.Src)

      // Apply the final rank updates to get the new ranks, using join to preserve ranks of vertices
      // that didn't receive a message. Requires a shuffle for broadcasting updated ranks to the
      // edge partitions.
      prevRankGraph = rankGraph
      rankGraph = rankGraph.joinVertices(rankUpdates) {
        // Set restart probability to personalized version(all restart will direct to source node)
        (id, oldRank, msgSum) => ((if (id == startPoint) alpha * numVertices else 0 ) + (1.0 - alpha) * msgSum / oldRank._2, oldRank._2)
      }.cache()

      rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      if (iteration % 5 == 0) println(s"PageRank finished iteration $iteration.")
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      iteration += 1

    }
    rankGraph
  }

  def revPageRank[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                              startPoint: VertexId,
                                               maxIter: Int,
                                               alpha: Double): Graph[(Double, Int), Double] = {

    val weight = graph.numVertices

    // Reversed PageRank

    var revGraph: Graph[(Double, Int), Double] = graph
      // Because we reverse every edge in this graph, so we need use in-degree for edge weight normalization
      .outerJoinVertices(graph.inDegrees) { (vid, vdata, deg) => deg.getOrElse(0)}
      .mapTriplets( e => 1.0 / e.dstAttr, TripletFields.Dst)
      .mapVertices((id, attr) => {
      if (id == startPoint) weight.toDouble else 0.0
    }).outerJoinVertices(graph.outDegrees) { (vid, vdata, outDeg) => (vdata, outDeg.getOrElse(0))}

    var iteration = 0
    var prevRankGraph: Graph[(Double, Int), Double] = null
    while (iteration < maxIter) {
      revGraph.cache()
      val rankUpdates = revGraph.aggregateMessages[Double](
        ctx => {
          ctx.sendToSrc(ctx.dstAttr._1 * ctx.attr)
        }, _ + _, TripletFields.Dst)

      prevRankGraph = revGraph
      revGraph = revGraph.joinVertices(rankUpdates) {
        (id, oldRank, msgSum) => ((if (id == startPoint) alpha * weight else 0) + (1.0 - alpha) * msgSum / oldRank._2, oldRank._2)
      }.cache()

      revGraph.edges.foreachPartition(x => {})
      if (iteration % 5 == 0) println(s"PageRank finished iteration $iteration.")
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      iteration += 1
    }

    revGraph
  }
}
