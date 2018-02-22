package es.us.linkage

import org.apache.spark.rdd.RDD

class LinkageModel(_clusters: RDD[(Long, (Int, Int))]) extends Serializable {

  def clusters = _clusters


  //Devuelve el cluster al que pertenece el punto, o el punto en el caso en que no esté
  //  def getRealPoint(point: Int): Int = {
  //    var res = point
  //    var auxPoint = point
  //    var found = false
  //    val default = (-1, "")
  //    while (!found) {
  //      val aux = this.clusters
  //        .find(x => (x._2.head._1 == res || x._2.head._2 == res))
  //        .getOrElse(default)._1
  //        .asInstanceOf[Number].intValue()
  //      if (aux == -1) {
  //        found = true
  //      } else {
  //        res = aux
  //      }
  //    }
  //    res
  //  }

    def isCluster(point: Int): Boolean = {
//    clusters.exists(_._1 == point)
      clusters.countByKey().contains(point.toLong)
  }

  def isCluster(point: Int, totalPoints: Int): Boolean = {
    point > totalPoints
  }

  //Dado un punto de un cluster, devuelve todos los puntos de ese cluster
    def giveMePoints(point: Int): List[Int] = {
      var res = List[Int]()
      val aux = clusters.lookup(point.toLong).head // valor de una Key(point)
      if (isCluster(aux._1)) {
        res = res ::: giveMePoints(aux._1)
        if (isCluster(aux._2)) {
          res = res ::: giveMePoints(aux._2)
        } else {
          res = res ::: List(aux._2)
        }
      } else {
        if (isCluster(aux._2)) {
          res = res ::: giveMePoints(aux._2)
          res = res ::: List(aux._1)
        } else {
          res = res ::: List(aux._1, aux._2)
        }
      }

      res
    }

  def giveMeCluster(point: Int, totalPoints: Int, clusterBase: RDD[(Int, Int)]): Int ={
    var res = point
    if(clusterBase.count()!=0) {
      var rest = clusterBase.map {
        case (x, y) =>
          var aux = 0
          if (!isCluster(point, totalPoints)) {
            if ((x != point) && (y != point)) {
              aux = point
            } else if (x == point) {
              aux = y
            }
          }else if(x == point){
            aux = y
          }
          aux
      }.distinct().max()

      if(isCluster(rest,totalPoints)){
        rest = giveMeCluster(rest,totalPoints,clusterBase.filter(x => x._1 >= rest))
      }
      res = rest
    }
    res
  }


  def printSchema(separator: String): Unit = {
    println(clusters
      .sortBy(_._1)
      .map(x => s"${
        x._1
      },${
        x._2._1
      },${
        x._2._2
      }")
      .collect()
      .mkString(separator))
  }

  def saveSchema(destino: String) = {
    clusters
      .sortBy(_._1)
      .map(x => s"${
        x._1
      },${
        x._2._1
      },${
        x._2._2
      }")
      .coalesce(1, shuffle = true)
      .saveAsTextFile(destino + "Linkage-" + Utils.whatTimeIsIt())
  }

  def createClusters(destino: String, points: Int, numCluster: Int, totalPoints: RDD[Int]) = {

    var cluster = 0

    val minCluster = points + 1
    val topCluster = clusters.count()
    val aux = clusters.filterByRange(minCluster, minCluster+(topCluster-numCluster)).sortByKey(false)
      .map{
        case (x,y) =>
          cluster = x.toInt
          y
      }

    val firstPoint = aux.map(x => (x._1,cluster))
    val secondPoint = aux.map(x => (x._2,cluster))

    val rest = firstPoint.union(secondPoint)

    var aux2 = scala.collection.mutable.Map[Int,Int]()
//    var aux2 = totalPoints
    for(x <- totalPoints.collect()){
      val point2 = giveMeCluster(x, points, rest)
      aux2 += (x -> point2)
//      aux2 = totalPoints.map(y => point2)
    }

    val res = totalPoints.map(x => (x,aux2(x))).sortByKey()
    res
      .map(_.toString().replace("(", "").replace(")", ""))
      .coalesce(1, shuffle = true)
      .saveAsTextFile(destino + "Points-" + points + "-Cluster-" + numCluster)
  }
}
