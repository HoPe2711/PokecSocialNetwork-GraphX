package jobs

import graph.SocialGraph
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.VertexRDD
import org.apache.spark._

object SocialPageRankJob {

  def ranks(socialGraph: SocialGraph, tolerance: Double): VertexRDD[Double] =
    socialGraph.graph.pageRank(tol = tolerance).vertices

  def static(socialGraph: SocialGraph, tolerance: Double): VertexRDD[Double] =
    socialGraph.graph.staticPageRank(numIter = 20).vertices

  def handleResult(socialGraph: SocialGraph, ranks: VertexRDD[Double]) = {
    socialGraph.verts.join(ranks).sortBy({ case (_, rank) => rank }, ascending = false).take(10)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("team5_PageRankPocket")
    val sc = new SparkContext(conf)

    val socialGraph: SocialGraph = new SocialGraph(sc)
    val TOLERANCE: Double = 0.0001

    val topUsersDynamically = handleResult(socialGraph, ranks(socialGraph, TOLERANCE))
    val topUsersIterative = handleResult(socialGraph, static(socialGraph, TOLERANCE))

    println("") 
    println(s"Top 10 users in network counted with TOLERANCE until convergence $TOLERANCE:")
    topUsersDynamically foreach (f => println(s"Id: ${f._1} \tRank: ${f._2._2}"))
    
    println("") 
    println(s"Top 10 users in the network counted iteratively:")
    topUsersIterative foreach (f => println(s"Id: ${f._1} \tStatic: ${f._2._2}"))

    sc.stop()
  }
}
