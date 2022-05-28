package jobs

import graph.SocialGraph
import org.apache.log4j.{Level, Logger}
import org.apache.spark._

object SocialGraphJob {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("team5_PokecGraph")
    
    val sc = new SparkContext(conf)

    val graph = new SocialGraph(sc)

    println("") 
    println(s"Num Of Verties: ${graph.getNumVerties} and Num Of Edge: ${graph.getNumEdges}")

    println("") 
    println("Top 8 users with the largest number of in-degrees:")
    graph.getMostInConnectedUsers(8) foreach (f => println(s"Id: ${f._1} \tNum Of In Degrees: ${f._2._1} \tInfo User: ${f._2._2}"))

    println("")
    println("Top 8 users with the largest number of out-degrees:")
    graph.getMostOutConnectedUsers(8) foreach (f => println(s"Id: ${f._1} \tNum Of Out Degrees: ${f._2._1} \tInfo User: ${f._2._2}"))

    println("")
    println("Computing degrees of separation for user 2494: ")
    graph.degreeOfSeparationSingleUser(2494) foreach (f => println(s"Destination vertex: ${f._1} \tDistance: ${f._2._1} \tInfo User: ${f._2._2}"))

    println("")
    print("Computing degrees of separation for user 2494 and 82167: ")
    graph.degreeOfSeparationTwoUser(2494, 82167) foreach println

    println("")
    print("Number of Connected components: ")
    graph.socialConnectedComponent foreach println

    println("")
    println("Number of triangles passing through each vertex:")
    graph.socialTriangleCount foreach (f => println(s"Id: ${f._1} \tNum Of Triangles: ${f._2._2} \tInfo User: ${f._2._1}"))

    println("")
    print("Number of triangles: ")
    val totalTriangles = graph.socialGraphTriangleCount.collect().foldLeft(0)(_+_._2)/3
    println(totalTriangles)
    
    println("")
    print("Global Clustering Coefficient: ")
    println(graph.globalClusteringCoefficient)

    sc.stop()
  }
}