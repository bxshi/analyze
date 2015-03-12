package edu.nd.dsg.bshi

object Main {

  def main(args: Array[String]): Unit = {

    ForwardBackwardPPRRanking.load(args)
    ForwardBackwardPPRRanking.run()
  }
}
