package es.us.linkage

import org.apache.spark.rdd.RDD

/**
  * Created by Jose David on 15/01/2018.
  */

class LinkageModel(_clusters: RDD[(Long, (Int, Int))]) extends Serializable {

  def clusters = _clusters

  def isCluster(point: Int): Boolean = {
    clusters.countByKey().contains(point.toLong)
  }

  def isCluster(point: Int, totalPoints: Int): Boolean = {
    point > totalPoints
  }

  //Given a point in a cluster, return all points of that cluster
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

  def giveMePoints(point: Int, numberPoints: Int): Array[(Int,Int)] = {
    var rest = new Array[(Int,Int)](numberPoints*2)
    val aux = clusters.lookup(point.toLong).head
    val cont = clusters.sparkContext.longAccumulator("Accumulator Points")
    cont.add(0)
    if (isCluster(aux._1)) {
      rest :+ giveMePoints(aux._1,numberPoints)
      if (isCluster(aux._2)) {
        rest :+ giveMePoints(aux._2,numberPoints)
      } else {
        rest(cont.value.toInt) = (aux._2,point)
      }
    } else {
      if (isCluster(aux._2)) {
        rest :+ giveMePoints(aux._2,numberPoints)
        rest(cont.value.toInt) = (aux._1,point)
      } else {
        rest(cont.value.toInt) = (aux._1,point)
        cont.add(1)
        rest(cont.value.toInt) = (aux._2,point)
        cont.add(1)
      }
    }

    rest
  }

  def giveMePointsRDD(cluster: Int, numberPoints: Int): RDD[(Int,Int)] = {

    val aux = clusters.lookup(cluster.toLong).head
    var rest = clusters.sparkContext.emptyRDD[(Int,Int)]

    if(isCluster(aux._1,numberPoints)){
      rest = rest.union(giveMePointsRDD(aux._1,numberPoints))
      if(isCluster(aux._2,numberPoints)){
        rest = rest.union(giveMePointsRDD(aux._2,numberPoints))
      }else {
        rest = rest.union(clusters.sparkContext.parallelize(Seq((aux._2,cluster))))
      }
    } else {
      if(isCluster(aux._2,numberPoints)){
        rest = rest.union(giveMePointsRDD(aux._2,numberPoints))
      }else {
        rest = rest.union(clusters.sparkContext.parallelize(Seq((aux._1,cluster))))
        rest = rest.union(clusters.sparkContext.parallelize(Seq((aux._2,cluster))))
      }
    }

    rest.sortByKey().filter(value => value._1 > 0).map(x => (x._1,cluster))

  }

  def giveMeCluster(point: Int, totalPoints: Int, clusterBase: RDD[(Int, Int)]): Int = {
    var rest = point
    if (clusterBase.count() != 0) {
      var pointResult = clusterBase.filter(x => x._1 >= point).map {
        case (x, y) =>
          var auxPoint = point
          if (!isCluster(point, totalPoints)) {
            if (x == point) {
              auxPoint = y
            }
          } else if (x == point) {
            auxPoint = y
          }
          auxPoint
      }.distinct().max()

      if (isCluster(pointResult, totalPoints) && pointResult != point) {
        pointResult = giveMeCluster(pointResult, totalPoints, clusterBase.filter(x => x._1 >= pointResult))
      }

      rest = pointResult
    }

    rest
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

  def saveResult(destino: String, resultPoints: RDD[(Int,Int)], numPoints: Int, numCluster: Int) = {
    resultPoints
      .sortByKey()
      .map(_.toString().replace("(", "").replace(")", ""))
      .coalesce(1, shuffle = true)
      .saveAsTextFile(destino + "Points-" + numPoints + "-Clusters-" + numCluster)
  }

  def createClusters(destino: String, points: Int, numCluster: Int, totalPoints: RDD[Int]) = {

    val sc = totalPoints.sparkContext

    //We filter the total of clusters establishing a lower and upper limit depending on the number of points and the level at which we want to stop
    val minCluster = points + 1
    val topCluster = clusters.count()

    val clustersFiltered = clusters.filterByRange(minCluster, minCluster + (topCluster - numCluster)).sortByKey().cache()

    //We generate an auxiliary RDD to start each cluster at each point
    var auxPoints = totalPoints.map(value => (value,value))
    var a = 0

    //We go through each row of the filtered cluster file
    for (iter <- clustersFiltered.collect()){
      val start = System.nanoTime

      //We save the elements of each row in auxiliary variables to be able to filter later
      val point1 = iter._2._1
      val point2 = iter._2._2
      val cluster = iter._1.toInt

      //We go through the auxiliary RDD and check if in this iteration it is necessary to change the cluster to which each point belongs
      auxPoints = auxPoints.map {value =>
        var auxValue = value
        if(value._2 == point1 || value._2 == point2){
          auxValue = (value._1, cluster)
        }
        auxValue
      }

      val duration = (System.nanoTime - start) / 1e9d
      println(s"TIME ITERATION: $duration")
      a = a + 1

      //Every two hundred iterations we make a checkpoint so that the memory does not overflow
      if(a % 200 == 0){
        auxPoints.checkpoint()
        auxPoints.count()
      }
    }

    //We save the result of clustering in an external file
    auxPoints
      .map(_.toString().replace("(", "").replace(")", ""))
      .coalesce(1, shuffle = true)
      .saveAsTextFile(destino + "Points-" + points + "-Clusters-" + numCluster)
  }
}
