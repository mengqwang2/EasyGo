import org.apache.spark.graphx._
// Import random graph generation library
import org.apache.spark.graphx.util.GraphGenerators
// A graph with edge attributes containing distances
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkContext, SparkConf}
object SSSP {
  def main(args: Array[String]){
    val app = "sssp"
    val numVertices = 100
    val numEPart: Option[Int] = None
    val partitionStrategy: Option[PartitionStrategy] = None
    val mu: Double = 4.0
    val sigma: Double = 1.3
    val degFile: String = ""
    val conf = new SparkConf().setAppName(s"GraphX Synth Benchmark (nverts = $numVertices, app = $app)").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryo.registrator", "org.apache.spark.graphx.GraphKryoRegistrator")
    val sc = new SparkContext(conf)
    val graph: Graph[Array[Long], Int] = GraphGenerators.logNormalGraph(sc, numVertices
, numEPart.getOrElse(sc.defaultParallelism), mu, sigma)
    val sourceId: VertexId = 42 // The ultimate source
    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
        (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
        triplet => {  // Send Message
          if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
            Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
          } else {
            Iterator.empty
          }
        },
        (a,b) => math.min(a,b) // Merge Message
    )
    println(sssp.vertices.collect.mkString("\n"))
  }
}