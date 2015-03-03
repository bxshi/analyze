package edu.nd.dsg.bshi

trait ExperimentTemplate {
  def load(args: Array[String]): Unit
  def run(): Unit
}
