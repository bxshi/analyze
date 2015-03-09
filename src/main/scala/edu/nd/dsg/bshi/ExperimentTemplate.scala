package edu.nd.dsg.bshi

import org.apache.spark.{SparkConf, SparkContext}

trait ExperimentTemplate extends ArgLoader {
  def load(args: Array[String]): Unit
  def run(): Unit

  def createSparkInstance(): SparkContext = {
    val conf = new SparkConf()
//      .setMaster("local[" + config.nCores.toString + "]")
      .setAppName("Citation PageRank")
      .set("spark.executor.memory", "40g")
      .set("spark.driver.memory", "40g")
      .set("spark.driver.maxResultSize", "20g")
    new SparkContext(conf)
  }

}
