package edu.nd.dsg.bshi

import org.apache.spark.graphx.lib.PageRank._
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

object Main {

  val alpha = 0.15
  val queryId = 1l //1326293

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Simple Application")
      .set("spark.executor.memory", "1g")
      .set("spark.driver.memory", "1g")
      .set("spark.driver.maxResultSize", "1g")
    val sc = new SparkContext(conf)

    val filePath = "hdfs://dsg1.crc.nd.edu/user/bshi/dblp/citation_edge.txt"
//    val filePath = "/Users/bshi/exges.txt"

    val file = sc.textFile(filePath).map(x => x.split(" ").map(_.toInt))

    val vertices: RDD[(VertexId, (Double,Boolean))] = sc.parallelize(file.map(_.toSet).reduce(_++_).toSeq.map(x => (x.toLong, (0.0, false))))

    val edges: RDD[Edge[Boolean]] = sc.textFile(filePath).map(x => {
      val endPoints = x.split(" ").map(_.toInt)
      Edge(endPoints(0), endPoints(1), true)
    })

    val graph = Graph(vertices, edges)

    val totalV = graph.numVertices

    val zeroDeg = graph.outDegrees.filter(_==0l).count()

      println(zeroDeg,"totalV",totalV)

    //TODO: Add edges to query point

    var rankGraph: Graph[(Double, Int), Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr, TripletFields.Src )
      // Set the vertex attributes to the initial pagerank values
      .mapVertices( (id, attr) => {
      if (id == queryId) totalV.toDouble else 0.0
    } ).outerJoinVertices(graph.inDegrees) { (vid, vdata, inDeg) => (vdata, inDeg.getOrElse(0))}

    var iteration = 0
    var prevRankGraph: Graph[Double, Double] = null
    while (iteration < 5) {
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
        (id, oldRank, msgSum) => alpha + (1.0 - alpha) * msgSum
      }.cache()

      rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      println(s"PageRank finished iteration $iteration.")
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      iteration += 1

//      val tmpGraph = rankGraph.outerJoinVertices(rankGraph.inDegrees){
//        (id, attr, inDeg) => inDeg match {
//          case Some(inDeg) => attr / inDeg
//          case None => attr
//        }
//      }

      sc.parallelize(rankGraph.vertices.map(x => (x._2, x._1)).top(20)).saveAsTextFile("hdfs://dsg1.crc.nd.edu/user/bshi/dblp/005_"+iteration+"_top20")

    }

    sc.stop()
  }
}
