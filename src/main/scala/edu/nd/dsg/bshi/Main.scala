package edu.nd.dsg.bshi

import edu.nd.dsg.bshi.exp.{ForwardBackwardPPRRanking, FindCouplingNodes}
import edu.nd.dsg.bshi.lib.ArgLoader

object Main extends ArgLoader[String] {

  def main(args: Array[String]): Unit = {

    argLoader(args)

    config.exp match {
      case "fbppr" => {
        ForwardBackwardPPRRanking.load(args)
        ForwardBackwardPPRRanking.run()
      }
      case "coupling" => {
        FindCouplingNodes.load(args)
        FindCouplingNodes.run()
      }
      case _ => println("unsupported operation", config.exp)
    }
  }
}
