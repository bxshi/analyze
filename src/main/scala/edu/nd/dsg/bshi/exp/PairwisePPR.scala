//package edu.nd.dsg.bshi.exp
//
//import edu.nd.dsg.bshi.lib.{DataExtractor, ExperimentTemplate, OutputWriter, PersonalizedPageRank}
//import org.apache.spark.graphx._
//
//import scala.collection.mutable
//
//
//object PairwisePPR extends ExperimentTemplate[Double] with OutputWriter[String] {
//
//  val stringKeys = Seq("title")
//
//  override def load(args: Array[String]): Unit = {
//    argLoader(args)
//    val sc = createSparkInstance()
//
//    if (!config.titlePath.isEmpty) {
//      titleMap = sc.textFile(config.titlePath).map(x => {
//        val tmp = x.split("\",\"").toList
//        Map[VertexId, String]((tmp(0).replace("\"", "").toLong, tmp(1).replaceAll("\\p{P}", " ")))
//      }).reduce(_ ++ _)
//    }
//
//    val file = sc.textFile(config.filePath).filter(!_.contains("#"))
//      .map(x => x.split("\\s").map(_.toInt))
//
//    vertices = sc.parallelize(file.map(_.toSet).reduce(_ ++ _).toSeq.map(x => (x.toLong, 0.0)))
//
//    edges = file.map(x => Edge(x(0), x(1), true))
//
//    graph = Graph(vertices, edges)
//
//    Seq("","Graph statistics:",
//      "--Vertices: " + graph.numVertices,
//      "--Edges:    " + graph.numEdges).foreach(println)
//
//    if (graph.vertices.filter(_._1 == config.queryId).count() == 0) {
//      println("QueryId ", config.queryId, " does not exist!")
//      sc.stop()
//      return
//    }
//  }
//
//  override def run(): Unit = {
//
//    val fpprGraph = PersonalizedPageRank.runWithInitialScore(
//      graph.mapVertices((vid, vd) => if(vid == config.queryId) 1.0 else 0.0),
//      config.queryId, config.maxIter, config.alpha)
//
//    val fpprTopK = DataExtractor.extractTopKFromPageRank(fpprGraph, titleMap, config.topK)
//    val candidateSet = fpprTopK.map(_._1).toSet
//
//    // Log top K + 1
//    fpprTopK.foreach(elem => {
//      if (!finalResult.contains(elem._1)) {
//        finalResult(elem._1) = new mutable.HashMap[String, String]()
//
//      }
//      finalResult(elem._1)("title") = elem._2
//      finalResult(elem._1)(config.queryId.toString+"_score") = elem._4.toString
//      finalResult(elem._1)(config.queryId.toString+"_rank") = elem._3.toString
//    })
//
//    fpprTopK.filter(_._1 != config.queryId).foreach(elem => {
//      val revGraph = graph.reverse.mapVertices((vid, vd) => if(vid == elem._1) 1.0 else 0.0)
//      val bpprGraph = PersonalizedPageRank.runWithInitialScore(revGraph, elem._1, config.maxIter, config.alpha)
//
//      DataExtractor.extractNodeFromPageRank(bpprGraph, titleMap, candidateSet).foreach(x => {
//        finalResult(x._1)(elem._1.toString+"_score") = x._4.toString
//        finalResult(x._1)(elem._1.toString+"_rank") = x._3.toString
//      })
//    })
//
//    val finalKeys = stringKeys ++
//      candidateSet.map(_.toString+"_rank").toSeq ++
//      candidateSet.map(_.toString+"_score").toSeq
//
////    printResult(finalResult, finalKeys)
//    writeResult(config.outPath, finalResult)
//
//  }
//
//}
