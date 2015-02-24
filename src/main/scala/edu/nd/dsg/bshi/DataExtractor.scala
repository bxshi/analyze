package edu.nd.dsg.bshi

import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
 * Extract data from Graph object
 *
 * TODO: Write test case
 */
object DataExtractor {

  /**
   * Extract topK nodes from PageRank result graph
   * @param graph the pagerank graph
   * @param titleMap the map that contains title of each node
   * @param top the number of nodes that will return (topK)
   * @tparam ED edge data type of original graph, not used in this method
   * @return a sequence of (vid, title of that vid, rank, score)
   */
  def extractTopKFromPageRank[ED: ClassTag](graph: Graph[Double, ED],
                                    titleMap: Map[VertexId, String],
                                    top: Int = 100): Seq[(VertexId, String, Int, Double)] = {

    val tmpRes = graph.vertices.map(v => (v._2, v._1)) // map to (PPR_score, vid)
      .top(top) // get topK
      .toSeq // Convert to sequence

    tmpRes.zip(tmpRes.indices) // zip with index
      .map(x => (x._1._2, titleMap.getOrElse(x._1._2, ""), x._2, x._1._1)) // (vid, title of that vid, rank, score)
  }

  /**
   * Extract a set of nodes from PageRank result graph
   * @param graph the pagerank graph
   * @param titleMap the map that contains title of each node
   * @param idSet A set of nodes that will return
   * @tparam ED edge data type of original graph, not used in this method
   * @return a sequence of (vid, title of that vid, rank, score)
   */
  def extractNodeFromPageRank[ED: ClassTag](graph: Graph[Double, ED],
                                              titleMap: Map[VertexId, String],
                                              idSet: Set[VertexId]): Seq[(VertexId, String, Int, Double)] = {
    val tmpRes = graph.vertices.sortBy(_._2, ascending = false) // Sort all nodes by its PR score in descending order
      .collect() // collect to master, this may be very expensive

    tmpRes.zip(tmpRes.indices) // zip with index
      .filter(e => idSet.contains(e._1._1)) // get all nodes that in idSet
      .map(x => (x._1._1, titleMap.getOrElse(x._1._1, ""), x._2, x._1._2)) // (vid, title of that vid, rank, score)
  }

  /**
   * Extract a node from PageRank result graph
   * @param graph the pagerank graph
   * @param titleMap the map that contains title of each node
   * @param source the node that will return
   * @tparam ED edge data type of original graph, not used in this method
   * @return a sequence of (vid, title of that vid, rank, score), the length of that sequence should always be 1
   */
  def extractNodeFromPageRank[ED: ClassTag](graph: Graph[Double, ED],
                                                     titleMap: Map[VertexId, String],
                                                     source: VertexId): Seq[(VertexId, String, Int, Double)] = {
    val tmpRes = graph.vertices.sortBy(_._2, ascending = false) // Sort all nodes by its PR score in descending order
      .collect() // collect to master, this may be very expensive

    tmpRes.zip(tmpRes.indices) // zip with index
      .filter(_._1._1 == source) // get only source node
      .map(x => (x._1._1, titleMap.getOrElse(x._1._1, ""), x._2, x._1._2))
  }

}
