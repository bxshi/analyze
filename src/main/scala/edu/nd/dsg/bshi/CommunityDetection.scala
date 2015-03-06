package edu.nd.dsg.bshi

import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.graphx.{Edge, VertexId, Graph}

import scala.collection.mutable


/**
 * Output community detection for certain iterations
 */
object CommunityDetection extends ExperimentTemplate with ArgLoader with OutputWriter[String] {

  override def load(args: Array[String]): Unit = {
    argLoader(args)

    val sc = createSparkInstance()

    val file = sc.textFile(config.filePath).filter(!_.contains("#"))
      .map(x => x.split("\\s").map(_.toInt))

    vertices = sc.parallelize(file.map(_.toSet).reduce(_ ++ _).toSeq.map(x => (x.toLong, 0.0)))

    edges = file.map(x => Edge(x(0), x(1), true))

    graph = Graph(vertices, edges)

    Seq("","Graph statistics:",
      "--Vertices: " + graph.numVertices,
      "--Edges:    " + graph.numEdges).foreach(println)
  }

  override def run(): Unit = {

    Range(config.lpaStart, config.lpastep, config.lpaEnd).foreach(iter => {
      val resGraph = LabelPropagation.run(graph, 10)

      resGraph.vertices.countByValue().foreach(println)

      resGraph.vertices.foreach(x => {
        if (!finalResult.contains(x._1)) {
          finalResult(x._1) = new mutable.HashMap[String, String]()
        }
        finalResult(x._1)(iter.toString + "iter") = x._2.toString
      })
    })

    writeResult(config.outPath, finalResult, Range(config.lpaStart, config.lpastep, config.lpaEnd).map(_.toString+"iter"))

  }
}
