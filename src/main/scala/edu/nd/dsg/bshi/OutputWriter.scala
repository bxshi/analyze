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
   * @param stringKeys a list of keys in HashMap
   */
  def writeResult(filePath: String,
                  resMap: mutable.HashMap[VertexId, mutable.HashMap[String, T]],
                  stringKeys: Seq[String]): Unit = {
    val writer = new PrintWriter(new File(filePath))

    writer.write(resToString(resMap, stringKeys))

    writer.close()

  }

  def resToString(resMap: mutable.HashMap[VertexId, mutable.HashMap[String, T]],
               stringKeys: Seq[String]): String = {
    val str = new mutable.StringBuilder
    str.append((Seq("id") ++ stringKeys).reduce(_+","+_)+"\n")
    resMap.map(elem => {
      val tuple = Seq(elem._1.toString) ++ stringKeys.map(x => elem._2.getOrElse(x, "NA").toString).map(_.replace(","," "))
      str.append(tuple.reduce(_+","+_)+"\n")
    })
    str.toString()
  }

}
