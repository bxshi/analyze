package edu.nd.dsg.bshi

import java.io.{File, PrintWriter}

import org.apache.spark.SparkContext
import org.apache.spark.graphx._

import scala.collection.mutable

/**
 * Implement Forward-Backward PPR here
 * Run FPPR first, get topK and log their values as (s_1, s_2, ..., s_k),
 * then run BPPR and log the values accordingly (s'_1, s'_2, ..., s'_k).
 * Calculate final score of topK as (\lambda * s_1/(s) + (1-\lambda)*s'_1/(s'), ...)
 * s and s' will be the initial score of source or the total score of entire graph in FPPR and BPPR
 */
object ForwardBackwardPPRRanking {

  var graph: Graph = null // original graph
  // final result {vid:{key1:val1, key2:val2, ...}}
  val finalResult = mutable.HashMap[VertexId, mutable.HashMap[String, String]]()
  // Keys that we will write
  val stringKeys = Seq("title", "fppr_score", "fppr_rank", "bppr_score", "bppr_rank")

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
    //TODO: Load graph

    //TODO: Run F-PPR, get topK result

    //TODO: Run B-PPR on all topK, get result

    //TODO: Combine them together, save to finalResult

    //TODO: Call writeResult to write results
  }

  /**
   * Write result to a file
   * @param filePath output file path
   */
  def writeResult(filePath: String): Unit = {
    val writer = new PrintWriter(new File(filePath))
    // write csv header
    writer.write((Seq("id") ++ stringKeys).reduce(_+","+_)+"\n")

    finalResult.map(elem => {
      val tuple = Seq(elem._1.toString) ++ stringKeys.map(x => elem._2.getOrElse(x, "NA").toString)
      writer.write(tuple.reduce(_+","+_)+"\n")
    })

    writer.close()

  }

}
