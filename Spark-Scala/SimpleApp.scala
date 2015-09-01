import org.apache.spark.graphx._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object GraphReader{
 def graphReader(path:String): Graph[Map[Int,Double],Double]={ //Map for d, vertex attr is weight
 val textFile = sc.textFile(path)
 val edges=textFile.map{
 	line=>
 	val fields=line.split("\\s+")
 	Edge(fields(0).toInt,fields(1).toInt,fields(2).toDouble)
 }
val graph:Graph[Map[Int,Double],Double]=Graph.fromEdges(edges,Map(0->0))
return graph
}
}
//GraphReader.graphReader("input.txt")



object Operator{
	def min(_in:Map[Int,Double],_ori:Map[Int,Double]):Map[Int,Double]={
		var result=_ori
		var it=_in.keysIterator
		var currentKey
		while(it.hasNext){
			currentKey=it.next()
			if(!(result contains currentKey)){
				result+=(currentKey -> _in(currentKey))
			}
			else{
				result+=(currentKey -> math.min(_in(currentKey),result(currentKey)))
			}
		}
		return result
	}

	def sum(_src:Map[Int,Double],_edgeWeight:Double):Map[Int,Double]={
		var result=_src
		var it=result.keysIterator
		var currentKey
		while(it.hasNext){
			currentKey=it.next()
			result+=(currentKey->(result(currentKey)+_edgeWeight))
		}
	return result
	}

	def lessthan(dstMap:Map[Int,Double],srcMap:Map[Int,Double],dist:Double):Boolean={
    var sendMsg = false
    val it = srcMap.keyIterator
    while(it.hasNext){
        var currentKey = it.next()
        if (dstMap.getOrElse(currentKey, Double.PositiveInfinity) > currentKey)){
            sendMsg = true
        }
    }
    return sendMsg
  }

}




object SSSP {
  def main(args: Array[String]){
  	//def run():Long={
    val app = "sssp"
    val numVertices = 100
    val numEPart: Option[Int] = None
    val partitionStrategy: Option[PartitionStrategy] = None

 	val graph:Graph[Map[Int,Double],Double]=GraphReader.graphReader("5.txt")

    val initialGraph = graph.mapVertices((id,_) => Map(id->0.0))
     val sssp = initialGraph.pregel(Map(-1,Double.PositiveInfinity))(
         (id, dist, newDist) => Operator.min(dist, newDist), // Vertex Program
         triplet => {  // Send Message
          if (Operator.lessthan(Operator.sum(triplet.srcAttr,triplet.attr),triplet.dstAttr)) {
            Iterator((triplet.dstId, Operator.sum(triplet.srcAttr,triplet.attr)))
          } else {
            Iterator.empty
          }
        },
        (a,b) => math.min(a,b) // Merge Message
    )
    // println(sssp.vertices.collect.mkString("\n"))
  }
}
