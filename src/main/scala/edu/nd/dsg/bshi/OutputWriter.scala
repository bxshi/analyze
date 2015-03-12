package edu.nd.dsg.bshi

import java.io.{File, PrintWriter}

import org.apache.spark.graphx._

import scala.collection.mutable


/**
 * Trait for csv output
 */
trait OutputWriter[T] {

  /**
   * Write result to a file
   * @param filePath output file path
   * @param resMap a nested output HashMap {id:{key:val, ...}, ...}
   */
  def writeResult(filePath: String,
                  resMap: mutable.HashMap[VertexId, mutable.HashMap[String, T]],
                  stringKeys: Seq[String] = Seq[String]()): Unit = {
    val writer = new PrintWriter(new File(filePath))

    writer.write(resToString(resMap, stringKeys))

    writer.close()

  }

  def resToString(resMap: mutable.HashMap[VertexId, mutable.HashMap[String, T]], stringKeys: Seq[String] = Seq[String]()): String = {
    val str = new mutable.StringBuilder
    val keys = if(resMap.size > 0) {
      resMap.map(_._2.keySet).reduce(_ ++ _) ++ stringKeys.toSet
    }.toSeq  else stringKeys
    str.append((Seq("id") ++ keys).reduce(_+","+_)+"\n")
    resMap.map(elem => {
      val tuple = Seq(elem._1.toString) ++ keys.map(x => elem._2.getOrElse(x, "NA").toString).map(_.replace(","," "))
      str.append(tuple.reduce(_+","+_)+"\n")
    })
    str.toString()
  }

  def printResult(resMap: mutable.HashMap[VertexId, mutable.HashMap[String, T]],
                  stringKeys: Seq[String] = Seq[String]()): Unit = {
    print(resToString(resMap, stringKeys))
  }

}
