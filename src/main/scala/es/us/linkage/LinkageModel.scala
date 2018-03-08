package es.us.linkage

import org.apache.spark.rdd.RDD

class LinkageModel(_clusters: RDD[(Long, (Int, Int))]) extends Serializable {

    def clusters = _clusters
//  var clusters = _clusters

//  def addClusters(newClusters: RDD[(Long, (Int, Int))]) = {
//    clusters = clusters.union(newClusters)
//  }


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
    clusters.countByKey().contains(point.toLong)
  }

  def isCluster(point: Int, totalPoints: Int): Boolean = {
    point > totalPoints
  }

  //Dado un punto de un cluster, devuelve todos los puntos de ese cluster
//  def giveMePoints(point: Int): List[Int] = {
//    var res = List[Int]()
//    val aux = clusters.lookup(point.toLong).head // valor de una Key(point)
//    if (isCluster(aux._1)) {
//      res = res ::: giveMePoints(aux._1)
//      if (isCluster(aux._2)) {
//        res = res ::: giveMePoints(aux._2)
//      } else {
//        res = res ::: List(aux._2)
//      }
//    } else {
//      if (isCluster(aux._2)) {
//        res = res ::: giveMePoints(aux._2)
//        res = res ::: List(aux._1)
//      } else {
//        res = res ::: List(aux._1, aux._2)
//      }
//    }
//
//    res
//  }

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

    //Filtramos el total de clusters estableciendo un límite inferior y superior en función del número de puntos y del nivel en el que queremos parar
    val minCluster = points + 1
    val topCluster = clusters.count()

    val clustersFiltered = clusters.filterByRange(minCluster, minCluster + (topCluster - numCluster)).sortByKey().cache()

    //Generamos un RDD auxiliar para iniciar cada cluster en cada punto
    var auxPoints = totalPoints.map(value => (value,value))
    val cont = sc.longAccumulator("My Accumulator")
    cont.add(0)

    //Recorremos cada fila del fichero de clusters filtrado
    for (iter <- clustersFiltered.collect()){
      val start = System.nanoTime

      //Guardamos en variables auxiliares los elementos de cada fila para poder hacer filtrados posteriores
      val point1 = iter._2._1
      val point2 = iter._2._2
      val cluster = iter._1.toInt

      //Recorremos el RDD auxiliar y comprobamos si en esta iteración es necesario cambiar el cluster al que pertenece cada punto
      auxPoints = auxPoints.map {value =>
        var auxValue = value
        if(value._2 == point1 || value._2 == point2){
          auxValue = (value._1, cluster)
        }
        auxValue
      }

      val duration = (System.nanoTime - start) / 1e9d
      println(s"TIME: $duration")
      cont.add(1)

      //Cada doscientas iteraciones hacemos un checkpoint para que la memoria no desborde
      if(cont.toString().toInt % 200 == 0){
        auxPoints.checkpoint()
        auxPoints.count()
      }
    }

    //Guardamos en un archivo externo el resultado del clustering
    auxPoints
      .map(_.toString().replace("(", "").replace(")", ""))
      .coalesce(1, shuffle = true)
      .saveAsTextFile(destino + "Points-" + points + "-Clusters-" + numCluster)
  }
}
