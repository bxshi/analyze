package edu.nd.dsg.bshi

object Main {

  def main(args: Array[String]): Unit = {

    CommunityDetection.load(args)
    CommunityDetection.run()
  }
}
