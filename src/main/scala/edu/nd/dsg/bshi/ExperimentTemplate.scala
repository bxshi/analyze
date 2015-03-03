package edu.nd.dsg.bshi

import org.apache.spark.{SparkConf, SparkContext}

trait ExperimentTemplate extends ArgLoader {
  def load(args: Array[String]): Unit
  def run(): Unit

  def createSparkInstance(): SparkContext = {
    val conf = new SparkConf()
      .setMaster("local[" + nCores.toString + "]")
      .setAppName("Citation PageRank")
      .set("spark.executor.memory", "1g")
      .set("spark.driver.memory", "1g")
      .set("spark.driver.maxResultSize", "1g")
    new SparkContext(conf)
  }

}
