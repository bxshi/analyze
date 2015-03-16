package edu.nd.dsg.bshi.test

import org.apache.spark.graphx._
import org.scalatest.{ShouldMatchers, FunSuite}

import edu.nd.dsg.bshi.OutputWriter

import scala.collection.mutable


class OutputWriterTest extends FunSuite with ShouldMatchers {

  def testOutputWriter = new OutputWriter[String] {}
  val testMap = mutable.HashMap[VertexId, mutable.HashMap[String, String]]()

  test("OutputWriter should write nothing") {
    val keys: Seq[String] = Seq.empty
    testOutputWriter.resToString(testMap, keys) should be ("id\n")
    testOutputWriter.resToString(testMap, Seq("key")) should be ("id,key\n")

  }

  test("OutputWriter should handle non-exist key correctly") {
    val keys: Seq[String] = Seq("a","b","c")
    testMap(0l) = mutable.HashMap[String, String]()
    testMap(0l)("a") = 1.toString
    testMap(0l)("b") = 225.toString

    testOutputWriter.resToString(testMap, keys) should be ("id,b,a,c\n0,225,1,NA\n")

  }

  ignore("Ignore key") {
    val keys: Seq[String] = Seq("a","b","c")

    testMap(1l) = mutable.HashMap[String, String]()
    testMap(1l)("a") = "aaa"
    testMap(1l)("b") = "bbb"
    testMap(1l)("c") = "ccc"
    testMap(1l)("d") = "won't appear"

    testOutputWriter.resToString(testMap).split("\n").toSet should be (Set("id,a,b,c","0,1,225,NA","1,aaa,bbb,ccc"))

  }

  test("Remove comma") {
    val keys: Seq[String] = Seq("a","b","c")
    testMap(2l) = mutable.HashMap[String, String]()
    testMap(2l)("a") = "a,a"
    testMap(2l)("b") = "b,b"
    testMap(2l)("c") = "c,c"
    testOutputWriter.resToString(testMap).split("\n").toSet should be (Set("id,b,a,c", "2,b b,a a,c c", "0,225,1,NA"))

  }
}
