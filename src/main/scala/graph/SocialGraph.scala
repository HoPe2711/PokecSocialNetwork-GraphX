package graph

import org.apache.spark._
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.rdd.RDD

class SocialGraph(sc: SparkContext) {
  type ConnectedUser = (PartitionID, InfoUser)
  type DegreeOfSeparation = (Double, InfoUser)

  def verts: RDD[(VertexId, InfoUser)] = sc.textFile(USER_NAMES).flatMap(InputDataFlow.parseNames)

  def edges: RDD[Edge[PartitionID]] = sc.textFile(USER_GRAPH).map { line =>
        val fields = line.split('\t')
        Edge(fields(0).toLong, fields(1).toLong, 0)
      }

  def graph = Graph(verts, edges).cache()

  def getNumEdges = graph.numEdges

  def getNumVerties = graph.numVertices

  def getMostInConnectedUsers(amount: Int): Array[(VertexId, ConnectedUser)] = {
    graph.inDegrees.join(verts)
      .sortBy({ case (_, (userName, _)) => userName }, ascending = false)
      .take(amount)
  }

  def getMostOutConnectedUsers(amount: Int): Array[(VertexId, ConnectedUser)] = {
    graph.outDegrees.join(verts)
      .sortBy({ case (_, (userName, _)) => userName }, ascending = false)
      .take(amount)
  }

  private def getBFS(root: VertexId) = {
    val initialGraph = graph.mapVertices((id, _) =>
      if (id == root) 0.0 else Double.PositiveInfinity)

    val bfs = initialGraph.pregel(Double.PositiveInfinity, maxIterations = 12)(
      (_, attr, msg) => math.min(attr, msg),
      triplet => {
        if (triplet.srcAttr != Double.PositiveInfinity) {
          Iterator((triplet.dstId, triplet.srcAttr + 1))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b)).cache()
    bfs
  }

  def degreeOfSeparationSingleUser(root: VertexId): Array[(VertexId, DegreeOfSeparation)] = {
    getBFS(root).vertices.join(verts)
    .filter { case (_, (userName, _)) => userName != Double.PositiveInfinity }
    .sortBy({ case (_, (userName, _)) => userName }, ascending = false)
    .take(10)
  }

  def degreeOfSeparationTwoUser(firstUser: VertexId, secondUser: VertexId) = {
    getBFS(firstUser)
      .vertices
      .filter { case (vertexId, _) => vertexId == secondUser }
      .collect.map { case (_, degree) => degree }
  }

  def socialConnectedComponent = graph.connectedComponents().vertices.map(x => (x._2, x._2)).countByKey()

  // def connectedComponentGroupedByUsers = verts.join(socialConnectedComponent)

  def socialGraphTriangleCount = graph.triangleCount().vertices

  def socialTriangleCount =  verts.join(socialGraphTriangleCount)
    .sortBy({ case (_, (_, userName)) => userName }, ascending = false).take(10)
    
  def globalClusteringCoefficient = {
      val numTriplets = graph.aggregateMessages[Set[VertexId]](
        et => { et.sendToSrc(Set(et.dstId));
                et.sendToDst(Set(et.srcId)) },
        (a,b) => a ++ b)
        .map(x => {val s = (x._2 - x._1).size; s*(s-1) / 2})
        .reduce(_ + _)
      if (numTriplets == 0) 0.0 else graph.triangleCount.vertices.map(_._2).reduce(_ + _) / numTriplets.toFloat
  }
}