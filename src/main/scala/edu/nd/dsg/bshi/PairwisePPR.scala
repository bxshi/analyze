package edu.nd.dsg.bshi

import org.apache.spark.graphx._

import scala.collection.mutable


object PairwisePPR extends ExperimentTemplate with OutputWriter[String] {

  val stringKeys = Seq("title")

  override def load(args: Array[String]): Unit = {
    argLoader(args)
    val sc = createSparkInstance()

    // Input file should be space separated e.g. "src dst", one edge per line
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

  override def run(): Unit = {

    val fpprGraph = PersonalizedPageRank.runWithInitialScore(
      graph.mapVertices((vid, vd) => if(vid == queryId) 1.0 else 0.0),
      queryId, maxIter, alpha)

    val fpprTopK = DataExtractor.extractTopKFromPageRank(fpprGraph, titleMap, topK)
    val candidateSet = fpprTopK.map(_._1).toSet

    // Log top K + 1
    fpprTopK.foreach(elem => {
      if (!finalResult.contains(elem._1)) {
        finalResult(elem._1) = new mutable.HashMap[String, String]()

      }
      finalResult(elem._1)("title") = elem._2
      finalResult(elem._1)(queryId.toString+"_score") = elem._4.toString
      finalResult(elem._1)(queryId.toString+"_rank") = elem._3.toString
    })

    fpprTopK.filter(_._1 != queryId).foreach(elem => {
      val revGraph = graph.reverse.mapVertices((vid, vd) => if(vid == elem._1) 1.0 else 0.0)
      val bpprGraph = PersonalizedPageRank.runWithInitialScore(revGraph, elem._1, maxIter, alpha)

      DataExtractor.extractNodeFromPageRank(bpprGraph, titleMap, candidateSet).foreach(x => {
        finalResult(x._1)(elem._1.toString+"_score") = x._4.toString
        finalResult(x._1)(elem._1.toString+"_rank") = x._3.toString
      })
    })

    val finalKeys = stringKeys ++
      candidateSet.map(_.toString+"_rank").toSeq ++
      candidateSet.map(_.toString+"_score").toSeq

//    printResult(finalResult, finalKeys)
    writeResult(outputPath, finalResult, finalKeys)

  }

}
