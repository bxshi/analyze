package edu.nd.dsg.bshi.exp

import java.text.{SimpleDateFormat, DateFormat}
import java.util.Locale

import edu.nd.dsg.bshi.lib.{ExperimentTemplate, OutputWriter}
import org.apache.spark.graphx._

import scala.collection.mutable


object FindCouplingNodes extends ExperimentTemplate[java.util.Date] with OutputWriter {

  var queryPairs: Array[(Long, Iterable[Long])] = null

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
      .map(x => x.split("[\\s,]").map(_.toInt))

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

    vertices = sc.parallelize(file.map(_.toSet).reduce(_ ++ _).toSeq.map(x => (x.toLong, dateFormat.parse("1900-01-01"))))

    val dates = sc.textFile(config.datePath)
                                  .filter(!_.contains("#"))
                                  .map(_.split("[\\s,]"))
                                  .map(x => (x(0).toLong, dateFormat.parse(x(1))))

    // combine date information
    vertices = VertexRDD(vertices).leftJoin(dates)(
      (vid, old_dt, new_dt) => new_dt.getOrElse(old_dt)
    )

    edges = if (config.directed) {
      file.map(x => Edge(x(0), x(1), true))
    } else { // Convert to undirected if not directed
      file.map(x => Edge(x(1), x(0), true)) ++ file.map(x => Edge(x(1), x(0), true))
    }

    graph = Graph(vertices.distinct(), edges.distinct()) // Make sure there is no duplication

    Seq("","Graph statistics:",
      "--Vertices: " + graph.numVertices,
      "--Edges:    " + graph.numEdges).foreach(println)

    queryPairs = sc.textFile(config.queryPath).filter(!_.contains("#"))
                  .map(x => x.split("[\\s,]").map(_.toLong)).groupBy(_(0)).map(i => (i._1, i._2.map(_.head))).collect()

    if (config.queryId != 0l && graph.vertices.filter(_._1 == config.queryId).count() == 0) {
      println("QueryId ", config.queryId, " does not exist!")
      sc.stop()
      return
    }
  }

  override def run(): Unit = {

    // Load query pairs
    queryPairs.foreach(queries => {
      val queryId = queries._1

      val queryNode = graph.vertices.filter(_._1 == queryId).first()

      finalResult((queryId,queryId)) = new mutable.HashMap[String, String]()

      val queryGraph = graph.outerJoinVertices(graph.collectNeighborIds(EdgeDirection.Out)){
        (vid, date, idList) => {
          var isValidNode = false
          var totalOutDeg = 0
          // check if this node reference queryNode
          if (idList.toSet.contains(queryId) ){//&& date.after(queryNode._2)) {
            // Count total out degree
            isValidNode = true
            totalOutDeg = idList.size
          }
          (date, isValidNode, totalOutDeg)
        }
      }

      println(queryId, "cited by "+queryGraph.vertices.filter(_._2._2).count()+" nodes")

      queries._2.foreach(target => {
        val resGraph = queryGraph.outerJoinVertices(queryGraph.collectNeighborIds(EdgeDirection.Out)){
          (vid, vd, idList) => {
            var appear = 0
            if (vd._2 && idList.toSet.contains(target)) {
              appear = 1
            }
            (vd._2, vd._3, appear)
          }
        }
        val num = resGraph.vertices.filter(_._2._1).count()
        val resPair = {
          val tmp = resGraph.vertices.filter(_._2._1).map(x=>(x._2._2, x._2._3))
          if(tmp.count() > 0) {
            tmp.reduce((a,b) => (a._1 + b._1, a._2 + b._2))
          } else {
            (0,0)
          }
        }

        finalResult((queryId,queryId))("appear") = num.toString // number of times that query been cite
        finalResult((queryId,queryId))("ncite") = resPair._1.toString // number of total cites
        finalResult((queryId,queryId))("coappear") = resPair._2.toString // number of co-appearance
        println(finalResult(queryId,queryId))
      })
    })

    writeResult(config.outPath, finalResult)

  }
}
