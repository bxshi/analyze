package edu.nd.dsg.bshi

import java.io.{File, PrintWriter}

import org.apache.spark.SparkContext
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
object ForwardBackwardPPRRanking extends OutputWriter[String]{

  var alpha = 0.15
  var queryId = 0l
  var filePath = ""
  var maxIter = 0
  var topK = 20
  var titlePath = ""
  var outputPath = ""
  var nCores = 4
  var vertices: RDD[(VertexId, Double)] = null
  var edges: RDD[Edge[Boolean]] = null
  var graph: Graph[Double,Boolean]=null  // original graph
  // final result {vid:{key1:val1, key2:val2, ...}}
  val finalResult = mutable.HashMap[VertexId, mutable.HashMap[String, String]]()
  // Keys that we will write
  val stringKeys = Seq("title", "fppr_score", "fppr_rank", "bppr_score", "bppr_rank")

  /**
   * Load graph and save to graph variable
   * @param args All the needed variables
   */
  def Loader(args: Array[String]): Unit = {
    //TODO: Implement graph loader
    alpha = args(0).toDouble
    queryId = args(1).toLong
    maxIter = args(2).toInt
    topK = args(3).toInt
    nCores = args(4).toInt
    filePath = args(5)
    titlePath = args(6)
    outputPath = args(7)
    args.foreach(elem => println(elem))
  }

  /**
   * Run FBPPR
   */
  def run(): Unit = {
    println("Run!!!")
    //TODO: Load graph

    //TODO: Run F-PPR, get topK result

    //TODO: Run B-PPR on all topK, get result

    //TODO: Combine them together, save to finalResult

    //TODO: Call writeResult to write results
    //writeResult("./test_output", finalResult, stringKeys)
  }

}
