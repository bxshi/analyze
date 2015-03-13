package edu.nd.dsg.bshi

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

  /**
   * Load graph and save to graph variable
   * @param args All the needed variables
   */
  def load(args: Array[String]): Unit = {

    argLoader(args) // Load all common arguments
    
    val sc = createSparkInstance()

    if (!config.titlePath.isEmpty) {
      titleMap = sc.textFile(config.titlePath).map(x => {
        val tmp = if(x.split("\",\"").toList.size == 1) {
          x.split("[\\s,]").toList
        } else x.split("\",\"").toList
        try{
          Map[VertexId, String]((tmp(0).replace("\"", "").toLong, tmp(1).replaceAll("\\p{P}", " ")))
        } catch {
          case _: Exception => {
            println(x, tmp, tmp.size)
            Map[VertexId, String]((tmp(0).replace("\"", "").toLong, "UNKNOWN_NODE"))
          }
        }
      }).reduce(_ ++ _)
    }

    // Input file should be space separated e.g. "src dst", one edge per line
    val file = sc.textFile(config.filePath).filter(!_.contains("#"))
      .map(x => x.split("\\s").map(_.toInt))

    vertices = sc.parallelize(file.map(_.toSet).reduce(_ ++ _).toSeq.map(x => (x.toLong, 0.0)))

    edges = if (config.directed) {
      file.map(x => Edge(x(0), x(1), true))
    } else { // Convert to undirected if not directed
      file.map(x => Edge(x(1), x(0), true)) ++ file.map(x => Edge(x(1), x(0), true))
    }

    graph = Graph(vertices.distinct(), edges.distinct()) // Make sure there is no duplication

    Seq("","Graph statistics:",
      "--Vertices: " + graph.numVertices,
      "--Edges:    " + graph.numEdges).foreach(println)

    if (config.queryId != 0l && graph.vertices.filter(_._1 == config.queryId).count() == 0) {
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

    val sampledNodes = if(config.queryId == 0l) {
      val tmp = graph.vertices.innerJoin(graph.outDegrees)((vid, data, outdeg) => outdeg)
        .filter(_._2 >= 10)

      println(tmp.count(), config.sample.toDouble,config.sample.toDouble / tmp.count())

      tmp.sample(false, if(config.sample.toDouble > tmp.count()) 1 else config.sample.toDouble / tmp.count(), 2l).map(_._1).collect().toSeq
    } else Seq(config.queryId)

    println("sample nodes are " + sampledNodes)

    sampledNodes.foreach(queryId => {
      var Initial_node = Array[Long](queryId)
      val newgraph = Graph(setInitialP(Initial_node), edges)
      val resGraph = PersonalizedPageRank.runWithInitialScore(newgraph, queryId, config.maxIter, config.alpha)
      val fpprRankTopK = DataExtractor.extractTopKFromPageRank(resGraph,titleMap, config.topK)

      fpprRankTopK.foreach(elem =>{
        if (!finalResult.contains(elem._1)) {
          finalResult(elem._1) = new mutable.HashMap[String, String]()
        }
          finalResult(elem._1)("title") = elem._2.toString
          finalResult(elem._1)("query_id") = queryId.toString
          finalResult(elem._1)("fppr_score")=elem._4.toString
          finalResult(elem._1)("fppr_rank")=elem._3.toString
      })

      fpprRankTopK.foreach(elem => {
        Initial_node = Array[Long](elem._1)
        val invgraph = Graph(setInitialP(Initial_node), graph.edges.reverse)
        val resGraph = PersonalizedPageRank.runWithInitialScore(invgraph, elem._1, config.maxIter, config.alpha)
        val ans = DataExtractor.extractNodeFromPageRank(resGraph, titleMap, queryId)
        finalResult(elem._1)("bppr_score") = ans.head._4.toString
        finalResult(elem._1)("bppr_rank") = ans.head._3.toString
        println(finalResult(elem._1))
        invgraph.unpersist(blocking = false)
        resGraph.unpersist(blocking = false)
      })
    })

    writeResult(config.outPath, finalResult)
    println("Finished!")
  }

}
