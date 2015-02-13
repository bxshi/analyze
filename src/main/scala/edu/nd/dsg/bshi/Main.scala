package edu.nd.dsg.bshi

import org.apache.spark.rdd.{RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

object Main {

  var alpha = 0.15
  var queryId = 0l
  var filePath = ""
  var maxIter = 0
  var topK = 20
  var titlePath = ""

  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      println("usage: alpha queryId maxIter topK edgeFilePath titleFilePath")
      return
    } else {
      alpha = args(0).toDouble
      queryId = args(1).toLong
      maxIter = args(2).toInt
      topK = args(3).toInt
      filePath = args(4)
      titlePath = args(5)
    }

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Citation PageRank")
      .set("spark.executor.memory", "1g")
      .set("spark.driver.memory", "1g")
      .set("spark.driver.maxResultSize", "1g")
    val sc = new SparkContext(conf)

    // Input file should be space separated e.g. "src dst", one edge per line
    val file = sc.textFile(filePath).map(x => x.split(" ").map(_.toInt))

    val vertices: RDD[(VertexId, (Double,Boolean))] = sc.parallelize(file.map(_.toSet).reduce(_++_).toSeq.map(x => (x.toLong, (0.0, false))))

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

    var rankGraph: Graph[(Double, Int), Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr, TripletFields.Src )
      // Set the vertex attributes to the initial pagerank values
      .mapVertices( (id, attr) => {
      // At initialization stage, only source has weight
      if (id == queryId) totalV.toDouble else 0.0
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
        (id, oldRank, msgSum) => ((if (id == queryId) alpha * totalV else 0 ) + (1.0 - alpha) * msgSum / oldRank._2, oldRank._2)
      }.cache()

      rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      if (iteration % 5 == 0) println(s"PageRank finished iteration $iteration.")
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      iteration += 1

    }

    val finalRank = rankGraph.vertices.map(x => x._2).top(topK).map(y => (y._2, y._1)).toSeq

    sc.stop()

    // Load article_list

    val titleMap = scala.io.Source.fromFile(titlePath).getLines().map(x => {
      val tmp = x.split("\",\"").toList
      Map[Int, String]((tmp(0).replace("\"","").toInt, tmp(1)))
    }).reduce(_++_)

    finalRank.indices.foreach(ind => println(ind, finalRank(ind), titleMap.get(finalRank(ind)._1)))


  }
}
