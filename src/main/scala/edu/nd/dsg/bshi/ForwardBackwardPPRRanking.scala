package edu.nd.dsg.bshi

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

/**
 * Implement Forward-Backward PPR here
 * Run FPPR first, get topK and log their values as (s_1, s_2, ..., s_k),
 * then run BPPR and log the values accordingly (s'_1, s'_2, ..., s'_k).
 * Calculate final score of topK as (\lambda * s_1/(s) + (1-\lambda)*s'_1/(s'), ...)
 * s and s' will be the initial score of source or the total score of entire graph in FPPR and BPPR
 */
object ForwardBackwardPPRRanking {

  //TODO: Implement functions to run F-B PPR

  var graph: Graph = null

  /**
   * Load graph and save to graph variable
   * @param sc SparkContext
   * @param filePaths A list of files that will be loaded
   */
  def graphLoader(sc: SparkContext, filePaths: Seq[String]): Unit = {
    //TODO: Implement graph loader
    graph = null
  }

  /**
   * Run FBPPR
   */
  def run(): Unit = {

  }

}
