package edu.nd.dsg.bshi

import java.io.{PrintWriter, File}

import org.apache.spark.rdd.{RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import scala.collection.mutable

object Main {

  def main(args: Array[String]): Unit = {

    if (args.length < 9) {
      println("usage: alpha queryId maxIter topK nCores edgeFilePath titleFilePath outputPath c")
      return
    }

    ForwardBackwardPPRRanking.load(args)
    ForwardBackwardPPRRanking.run()
  }
}
