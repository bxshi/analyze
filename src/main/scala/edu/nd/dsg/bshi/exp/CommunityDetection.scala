package edu.nd.dsg.bshi.exp

import edu.nd.dsg.bshi.lib.{ExperimentTemplate, OutputWriter}
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.graphx.{Edge, Graph, VertexId}

import scala.collection.mutable


/**
 * Output community detection for certain iterations
 */
object CommunityDetection extends ExperimentTemplate[Double] with OutputWriter {

  override def load(args: Array[String]): Unit = {
    argLoader(args)

    val sc = createSparkInstance()

    if (!config.titlePath.isEmpty) {
      titleMap = sc.textFile(config.titlePath).map(x => {
        val tmp = x.split("\",\"").toList
        Map[VertexId, String]((tmp(0).replace("\"", "").toLong, tmp(1).replaceAll("\\p{P}", " ")))
      }).reduce(_ ++ _)
    }

    val file = sc.textFile(config.filePath).filter(!_.contains("#"))
      .map(x => x.split("\\s").map(_.toInt))

    vertices = sc.parallelize(file.map(_.toSet).reduce(_ ++ _).toSeq.map(x => (x.toLong, 0.0)))

    edges = file.map(x => Edge(x(0), x(1), true)) ++ file.map(x => Edge(x(1), x(0), true))

    graph = Graph(vertices, edges)

    Seq("","Graph statistics:",
      "--Vertices: " + graph.numVertices,
      "--Edges:    " + graph.numEdges).foreach(println)
  }

  override def run(): Unit = {
    println("enter run")
    Range(config.lpaStart, config.lpaEnd, config.lpastep).foreach(iter => {
      println("LPA iter "+iter.toString)
      val resGraph = LabelPropagation.run(graph, iter)

      resGraph.vertices.collect().foreach(x => {
        if (!finalResult.contains((x._1,x._1))) {
          finalResult((x._1,x._1)) = new mutable.HashMap[String, String]()
        }
        finalResult((x._1,x._1))(iter.toString + "iter") = x._2.toString
      })
    })

    writeResult(config.outPath, finalResult)

  }
}
