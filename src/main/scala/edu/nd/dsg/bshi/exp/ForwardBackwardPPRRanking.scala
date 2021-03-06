package edu.nd.dsg.bshi.exp

import edu.nd.dsg.bshi.lib.{DataExtractor, ExperimentTemplate, OutputWriter, PersonalizedPageRank}
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
object ForwardBackwardPPRRanking extends ExperimentTemplate[Double] with OutputWriter {

  /**
   * Load graph and save to graph variable
   * @param args All the needed variables
   */
  override def load(args: Array[String]): Unit = {

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

    if (config.sampleEdge) { // edge based sampling
      // Get sampling node pairs by edges
      val sampleNodePairs = graph.triplets.sample(false,
        if (config.sample > graph.numEdges) 1 else config.sample.toDouble/graph.numEdges, 2l)
        .map(x => (x.srcId, x.dstId)).collect().toSeq

      println("sample edges are " + sampleNodePairs.map(x=>x._1.toString+","+x._2.toString).reduce(_++_))

      sampleNodePairs.foreach(nodePair => {
        var Initial_node = Array[Long](nodePair._1)
        val newgraph = Graph(setInitialP(Initial_node), edges)
        val resGraph = if(config.maxIter > 0) {
          PersonalizedPageRank.runWithInitialScore(newgraph, nodePair._1, config.maxIter, config.alpha)
        } else {
          PersonalizedPageRank.runWithInitialScoreUntilConvergence(newgraph, nodePair._1, tol = 0.00000001, config.alpha)
        }
        val fpprRank = DataExtractor.extractNodeFromPageRank(resGraph, titleMap, nodePair._2)

        newgraph.unpersist(blocking = false)

        if (!finalResult.contains(nodePair)) {
          finalResult(nodePair) = new mutable.HashMap[String, String]()
        }
        finalResult(nodePair)("src_title") = titleMap.getOrElse(nodePair._1,"")
        finalResult(nodePair)("dst_title") = fpprRank.head._2
        finalResult(nodePair)("query_id") = nodePair._1.toString
        finalResult(nodePair)("fppr_score")=fpprRank.head._4.toString
        finalResult(nodePair)("fppr_rank")=fpprRank.head._3.toString

        Initial_node = Array[Long](nodePair._2)
        val invgraph = Graph(setInitialP(Initial_node), graph.edges.reverse)
        val resGraph2 = if(config.maxIter > 0) {
          PersonalizedPageRank.runWithInitialScore(invgraph, nodePair._2, config.maxIter, config.alpha)
        } else {
          PersonalizedPageRank.runWithInitialScoreUntilConvergence(invgraph, nodePair._2, tol = 00000001, config.alpha)
        }
        val ans = DataExtractor.extractNodeFromPageRank(resGraph2, titleMap, nodePair._1)
        finalResult(nodePair)("bppr_score") = ans.head._4.toString
        finalResult(nodePair)("bppr_rank") = ans.head._3.toString
        println(finalResult(nodePair))
        invgraph.unpersist(blocking = false)
        resGraph.unpersist(blocking = false)

      })

    } else { // node based sampling
      val sampledNodes = if(config.queryId == 0l) {
        val tmp = graph.vertices.innerJoin(graph.outDegrees)((vid, data, outdeg) => outdeg)
          .innerJoin(graph.inDegrees)((vid, data, indeg) => (data, indeg))
          .filter(x=> x._2._1 >= 10 && x._2._2 >= 10)

        println(tmp.count(), config.sample.toDouble,config.sample.toDouble / tmp.count())

        tmp.sample(false, if(config.sample.toDouble > tmp.count()) 1 else config.sample.toDouble / tmp.count(), 2l).map(_._1).collect().toSeq
      } else Seq(config.queryId)

      println("sample nodes are " + sampledNodes)

      sampledNodes.foreach(queryId => {
        var Initial_node = Array[Long](queryId)
        val newgraph = Graph(setInitialP(Initial_node), edges)
        val resGraph = if (config.maxIter > 0) {
          PersonalizedPageRank.runWithInitialScore(newgraph, queryId, config.maxIter, config.alpha)
        } else {
          PersonalizedPageRank.runWithInitialScoreUntilConvergence(newgraph, queryId, tol=0.00000001, config.alpha)
        }
        val fpprRankTopK = DataExtractor.extractTopKFromPageRank(resGraph,titleMap, config.topK)

        fpprRankTopK.foreach(elem =>{
          if (!finalResult.contains((elem._1, queryId))) {
            finalResult((elem._1, queryId)) = new mutable.HashMap[String, String]()
          }
          finalResult((elem._1, queryId))("title") = elem._2.toString
          finalResult((elem._1, queryId))("query_id") = queryId.toString
          finalResult((elem._1, queryId))("fppr_score")=elem._4.toString
          finalResult((elem._1, queryId))("fppr_rank")=elem._3.toString
        })

        fpprRankTopK.foreach(elem => {
          Initial_node = Array[Long](elem._1)
          val invgraph = Graph(setInitialP(Initial_node), graph.edges.reverse)
          val resGraph = if(config.maxIter > 0) {
            PersonalizedPageRank.runWithInitialScore(invgraph, elem._1, config.maxIter, config.alpha)
          } else {
            PersonalizedPageRank.runWithInitialScoreUntilConvergence(invgraph, elem._1, tol=0.00000001, config.alpha)
          }
          val ans = DataExtractor.extractNodeFromPageRank(resGraph, titleMap, queryId)
          finalResult((elem._1, queryId))("bppr_score") = ans.head._4.toString
          finalResult((elem._1, queryId))("bppr_rank") = ans.head._3.toString
          println(finalResult((elem._1, queryId)))
          invgraph.unpersist(blocking = false)
          resGraph.unpersist(blocking = false)
        })
      })
    }

    writeResult(config.outPath, finalResult)
    println("Finished!")
  }

}
