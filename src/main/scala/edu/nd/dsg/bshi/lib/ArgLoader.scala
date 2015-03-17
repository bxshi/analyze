package edu.nd.dsg.bshi.lib

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable

trait ArgLoader[T] {

  var config: Config = null
  var vertices: RDD[(VertexId, T)] = null
  var edges: RDD[Edge[Boolean]] = null
  var graph: Graph[T,Boolean]=null  // original graph
  // final result {vid:{key1:val1, key2:val2, ...}}
  val finalResult = mutable.HashMap[VertexId, mutable.HashMap[String, String]]()
  var titleMap :Map[VertexId, String] = Map[VertexId, String]()

  // Config file
  case class Config(
                   // Algorithm related
                    alpha: Double = 0.15, // Damping factor of PR & PPR
                    maxIter: Int = 15, // Max iteration of PR & PPR
                    topK: Int = 20, // Return topK elements from PR & PPR
                    c: Int = 50, // Parameter for backward PPR, part of decay factor \beta
                    lpaEnd: Int = 50, // Max iteration of LPA community detection
                    lpastep: Int = 10, // Step of LPA iteration
                    lpaStart: Int = 10, // Min iteration of LPA
                   // Data related
                    queryId: VertexId = 0l,// Query id, usually the starting point of PPR
                    filePath: String = "", // Path of file, should be on hdfs
                    titlePath: String = "",// Path of title-id map, should be on local disk
                    datePath: String = "", // File contains date information
                    queryPath: String = "", // Path of query pairs
                    outPath: String = "", // Output path, should be a local location
                    sample: Int = 5, // Number of sample nodes
                    directed: Boolean = true, // Whether this is a directed graph or not
                   // Misc
                    nCores: Int = 4, // Number of cores for Spark
                    file: String = "", // log4j properties
                    exp: String = "" // Which experiment to run
   )

  val parser = new scopt.OptionParser[Config]("analyze") {
    head("analyze", "(who.cares.the.version - number RC)")

    opt[Double]('a', "alpha") action {
      (x,c) => c.copy(alpha = x)
    } text "Damping factor of PR & PPR default 0.15"

    opt[Int]('i', "iter") action {
      (x,c) => c.copy(maxIter = x)
    } text "Max iteration number of PR & PPR default 15"

    opt[Int]('k', "topk") action {
      (x,c) => c.copy(topK = x)
    } text "TopK elements that will return default 20"

    opt[Int]('c', "c") action {
      (x,c) => c.copy(c = x)
    } text "Parameter for backward PPR, part of decay factor \\beta default 50"

    opt[Int]("lpaStart") action {
      (x,c) => c.copy(lpaStart = x)
    } text "Min iteration of LPA"

    opt[Int]("lpaStep") action {
      (x,c) => c.copy(lpastep = x)
    } text "Step of LPA iteration"

    opt[Int]("lpaEnd") action {
      (x,c) => c.copy(lpaEnd = x)
    } text "Max iteration of LPA community detection"

    opt[Int]('s', "sample") action {
      (x,c) => c.copy(sample = x)
    } text "Number of random starting nodes, default is 5"

    opt[Boolean]('d', "directed") action {
      (x,c) => c.copy(directed = x)
    } text "directed graph or not, default is directed graph"

    opt[VertexId]('q', "query") action {
      (x,c) => c.copy(queryId = x)
    } text "Query id, usually the starting point of PPR, default is 0l"

    opt[String]('e', "edge") required() action {
      (x,c) => c.copy(filePath = x)
    } text "Path of edge file, should be on hdfs"

    opt[String]('t', "title") action {
      (x,c) => c.copy(titlePath = x)
    } text "Path of title-id map"

    opt[String]("date") action {
      (x,c) => c.copy(datePath = x)
    } text "Path of dates"

    opt[String]("queryPath") action {
      (x,c) => c.copy(queryPath = x)
    } text "Path of query pairs"

    opt[String]('o',"output") required() action {
      (x,c) => c.copy(outPath = x)
    } text "Output path, should be a local location"

    opt[Int]('n', "ncore") action {
      (x,c) => c.copy(nCores = x)
    } text "Number of cores for Spark"

    opt[String]("file") action {
      (x,c) => c.copy(file = x)
    } text "Log4j properties file"

    opt[String]("exp") required() action {
      (x,c) => c.copy(exp = x)
    } text "Which experiment to run"

  }

  def argLoader(args: Array[String]): Unit = {

    parser.parse(args, Config()) match {
      case Some(conf) =>

        // Some warnings
        if (conf.queryId == 0l) println("QueryId was not specified")
        if (conf.titlePath.isEmpty) println("TitleMap was not specified, there will be not title/text associated to ids")

        config = conf
      case None =>
        println("Here are some error about argument parsing, please check the source code and --help to make sure everything is fine.")
        System.exit(233)
    }

  }

}
