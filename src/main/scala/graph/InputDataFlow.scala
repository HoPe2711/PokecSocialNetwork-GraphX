package graph

import org.apache.spark.graphx.{Edge, VertexId}

import scala.collection.mutable.ListBuffer

object InputDataFlow {

  def parseNames(line: String): Option[(VertexId, InfoUser)] = {
    val fields = line.split('\t')
    if (fields(1) == "null") fields(1) = "-1"
    if (fields(5) == "null") fields(5) = "-1"
    val user:InfoUser = (fields(1).trim().toLong, fields(2), fields(5).trim().toLong)
    if (fields.length > 1)
      Some(fields(0).trim().toLong, user)
    else None
  }

}