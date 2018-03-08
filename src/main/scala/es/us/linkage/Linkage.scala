package es.us.linkage

import org.apache.spark.sql.functions.min
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
  * Created by Jose David on 15/01/2017.
  */
class Linkage(
               private var numClusters: Int,
               private var distanceStrategy: String) extends Serializable {

  def getNumClusters: Int = numClusters

  def setNumClusters(numClusters: Int): this.type = {
    this.numClusters = numClusters
    this
  }

  def getDistanceStrategy: String = distanceStrategy

  def setDistanceStrategy(distanceStrategy: String): this.type = {
    this.distanceStrategy = distanceStrategy
    this
  }

  // sort by dist
  object DistOrdering extends Ordering[Distance] {
    def compare(a: Distance, b: Distance) = a.getDist compare b.getDist
  }

  def runAlgorithm(distanceMatrix: RDD[Distance], numPoints: Int): LinkageModel = {

    var matrix = distanceMatrix
    val sc = distanceMatrix.sparkContext

    //Inicializamos el contador teniendo en cuenta el valor maximo de puntos existentes
    val cont = sc.longAccumulator("My Accumulator")
    cont.add(numPoints)
    val partitionNumber = distanceMatrix.getNumPartitions

    val linkageModel = scala.collection.mutable.Map[Long, (Int, Int)]()

    for (a <- 0 until (numPoints - numClusters)) {
      val start = System.nanoTime

      println("Finding minimum:")
      val clustersRes = matrix.min()(DistOrdering)

      println(s"New minimum: $clustersRes")

      val point1 = clustersRes.getIdW1
      val point2 = clustersRes.getIdW2
      cont.add(1)
      val newIndex = cont.value.toLong

      println("New Cluster: " + newIndex + ":" + point1 + "-" + point2)

      //Se guarda en el modelo resultado el nuevo cluster
      linkageModel += newIndex -> (point1, point2)

      //Si no es el ultimo cluster
      if (a < (numPoints - numClusters - 1)) {

        //Se elimina el punto encontrado
        matrix = matrix.filter(x => !(x.getIdW1 == point1 && x.getIdW2 == point2))

        //Buscamos todas las distancias que contengan la primera coordenada del nuevo cluster
        val rddPoints1 = matrix.filter(x => x.getIdW1 == point1 || x.getIdW2 == point1).collect()

        //Buscamos todas las distancias que contengan la segunda coordenada del nuevo cluster
        val rddPoints2 = matrix.filter(x => x.getIdW1 == point2 || x.getIdW2 == point2).collect()

        //Eliminamos los puntos completos
        val matrixSub = matrix.filter(x => !(x.getIdW1 == point1 || x.getIdW2 == point1))
          .filter(x => !(x.getIdW1 == point2 || x.getIdW2 == point2))

        val rddCartesianPoints = sc.parallelize(rddPoints1).cartesian(sc.parallelize(rddPoints2))
        val rddFilteredPoints = rddCartesianPoints.filter(x => (x._1.getIdW2 == x._2.getIdW2) ||
          (x._1.getIdW1 == x._2.getIdW1) ||
          (x._1.getIdW1 == x._2.getIdW2 ||
            (x._2.getIdW1 == x._1.getIdW2)))

        //Se crea un nuevo punto siguiendo la estrategia
        matrix = distanceStrategy match {

          case "min" =>
            //Se calcula la nueva distancia con respecto a todos los puntos y la estrategia escogida
            val newPoints = rddFilteredPoints.map(x => new Distance(newIndex.toInt, filterMatrix(x._1, clustersRes),
              math.min(x._1.getDist, x._2.getDist)))

            //Agrego los puntos con el nuevo indice
            matrixSub.union(newPoints)

          case "max" =>
            //Se calcula la nueva distancia con respecto a todos los puntos y la estrategia escogida
            val newPoints = rddFilteredPoints.map(x => new Distance(newIndex.toInt, filterMatrix(x._1, clustersRes),
              math.max(x._1.getDist, x._2.getDist)))

            matrixSub.union(newPoints)

          case "avg" =>
            //Se calcula la nueva distancia con respecto a todos los puntos y la estrategia escogida
            val newPoints = rddFilteredPoints.map(x => new Distance(newIndex.toInt, filterMatrix(x._1, clustersRes),
              ((x._1.getDist + x._2.getDist) / 2)))

            matrixSub.union(newPoints)
        }
      }

      matrix = matrix.coalesce(partitionNumber / 2).persist(StorageLevel.MEMORY_ONLY_2)

      if (a % 5 == 0) {
        matrix.checkpoint()
      }

      val duration = (System.nanoTime - start) / 1e9d
      println(s"TIME: $duration")
    }

    (new LinkageModel(sc.parallelize(linkageModel.toSeq)))

  }

  def runAlgorithmWithResult(distanceMatrix: RDD[Distance], numPoints: Int): (LinkageModel,RDD[(Int,Int)]) = {

    var matrix = distanceMatrix
    val sc = distanceMatrix.sparkContext

    //Inicializamos el contador teniendo en cuenta el valor maximo de puntos existentes
    val cont = sc.longAccumulator("My Accumulator")
    cont.add(numPoints)
    val partitionNumber = distanceMatrix.getNumPartitions

    val linkageModel = scala.collection.mutable.Map[Long, (Int, Int)]()

    val auxPoints = sc.parallelize(1 to numPoints)
    var totalPoints = auxPoints.map(value => (value,value)).cache()

    for (a <- 0 until (numPoints - numClusters)) {
      val start = System.nanoTime

      println("Finding minimum:")
      val clustersRes = matrix.min()(DistOrdering)

      println(s"New minimum: $clustersRes")

      val point1 = clustersRes.getIdW1
      val point2 = clustersRes.getIdW2
      cont.add(1)
      val newIndex = cont.value.toLong

      println("New Cluster: " + newIndex + ":" + point1 + "-" + point2)

      //Se guarda en el modelo resultado el nuevo cluster
      linkageModel += newIndex -> (point1, point2)

      totalPoints = totalPoints.map {value =>
        var auxValue = value
        if(value._2 == point1 || value._2 == point2){
          auxValue = (value._1, newIndex.toInt)
        }
        auxValue
      }

      //Si no es el ultimo cluster
      if (a < (numPoints - numClusters - 1)) {

        //Se elimina el punto encontrado
        matrix = matrix.filter(x => !(x.getIdW1 == point1 && x.getIdW2 == point2))

        //Buscamos todas las distancias que contengan la primera coordenada del nuevo cluster
        val rddPoints1 = matrix.filter(x => x.getIdW1 == point1 || x.getIdW2 == point1).collect()

        //Buscamos todas las distancias que contengan la segunda coordenada del nuevo cluster
        val rddPoints2 = matrix.filter(x => x.getIdW1 == point2 || x.getIdW2 == point2).collect()

        //Eliminamos los puntos completos
        val matrixSub = matrix.filter(x => !(x.getIdW1 == point1 || x.getIdW2 == point1))
          .filter(x => !(x.getIdW1 == point2 || x.getIdW2 == point2))

        val rddCartesianPoints = sc.parallelize(rddPoints1).cartesian(sc.parallelize(rddPoints2))
        val rddFilteredPoints = rddCartesianPoints.filter(x => (x._1.getIdW2 == x._2.getIdW2) ||
          (x._1.getIdW1 == x._2.getIdW1) ||
          (x._1.getIdW1 == x._2.getIdW2 ||
            (x._2.getIdW1 == x._1.getIdW2)))

        //Se crea un nuevo punto siguiendo la estrategia
        matrix = distanceStrategy match {

          case "min" =>
            //Se calcula la nueva distancia con respecto a todos los puntos y la estrategia escogida
            val newPoints = rddFilteredPoints.map(x => new Distance(newIndex.toInt, filterMatrix(x._1, clustersRes),
              math.min(x._1.getDist, x._2.getDist)))

            //Agrego los puntos con el nuevo indice
            matrixSub.union(newPoints)

          case "max" =>
            //Se calcula la nueva distancia con respecto a todos los puntos y la estrategia escogida
            val newPoints = rddFilteredPoints.map(x => new Distance(newIndex.toInt, filterMatrix(x._1, clustersRes),
              math.max(x._1.getDist, x._2.getDist)))

            matrixSub.union(newPoints)

          case "avg" =>
            //Se calcula la nueva distancia con respecto a todos los puntos y la estrategia escogida
            val newPoints = rddFilteredPoints.map(x => new Distance(newIndex.toInt, filterMatrix(x._1, clustersRes),
              ((x._1.getDist + x._2.getDist) / 2)))

            matrixSub.union(newPoints)
        }
      }

      matrix = matrix.coalesce(partitionNumber / 2).persist(StorageLevel.MEMORY_ONLY_2)

      if (a % 5 == 0) {
        matrix.checkpoint()
      }

      //      if (a % 20 == 0){
      //        totalPoints.checkpoint()
      //        totalPoints.count()
      //      }

      if (a % 200 == 0){
        totalPoints.map(_.toString().replace("(", "").replace(")", "")).coalesce(1).saveAsTextFile("B:\\Datasets\\saves" + "Clustering-" + a)
        totalPoints = sc.textFile("B:\\Datasets\\saves" + "Clustering-" + a).map(s => s.split(',').map(_.toInt))
          .map { case x =>
            (x(0), x(1))
          }
      }

      val duration = (System.nanoTime - start) / 1e9d
      println(s"TIME: $duration")
    }

    (new LinkageModel(sc.parallelize(linkageModel.toSeq)), totalPoints)

  }


  def runAlgorithmDendrogram(distanceMatrix: RDD[Distance], numPoints: Int, numClusters: Int) = {

    var matrix = distanceMatrix
    val sc = distanceMatrix.sparkContext

    //Inicializamos el contador teniendo en cuenta el valor maximo de puntos existentes
    val cont = sc.longAccumulator("My Accumulator")
    cont.add(numPoints)
    val partitionNumber = distanceMatrix.getNumPartitions

    val linkageModel = scala.collection.mutable.Map[Long, (Int, Int)]()
    var auxLinkageModel = new LinkageModel(sc.parallelize(linkageModel.toSeq))

    var model = sc.emptyRDD[(Double,Double,Double,Double)]

    for (a <- 0 until (numPoints - numClusters)) {
      val start = System.nanoTime

      println("Finding minimum:")
      val clustersRes = matrix.min()(DistOrdering)

      println(s"New minimum: $clustersRes")

      val point1 = clustersRes.getIdW1
      val point2 = clustersRes.getIdW2
      val dist = clustersRes.getDist
      cont.add(1)
      val newIndex = cont.value.toInt

      println("New Cluster: " + newIndex + ":" + point1 + "-" + point2)

      linkageModel += newIndex.toLong -> (point1, point2)

      auxLinkageModel = new LinkageModel(sc.parallelize(linkageModel.toSeq))

      model = model.union(sc.parallelize(Seq((point1.toDouble,point2.toDouble,dist.toDouble,
        auxLinkageModel.giveMePointsRDD(newIndex,numPoints).count().toDouble))))

      //Si no es el ultimo cluster
      if (a < (numPoints - numClusters - 1)) {

        //Se elimina el punto encontrado
        matrix = matrix.filter(x => !(x.getIdW1 == point1 && x.getIdW2 == point2))

        //Buscamos todas las distancias que contengan la primera coordenada del nuevo cluster
        val rddPoints1 = matrix.filter(x => x.getIdW1 == point1 || x.getIdW2 == point1).collect()

        //Buscamos todas las distancias que contengan la segunda coordenada del nuevo cluster
        val rddPoints2 = matrix.filter(x => x.getIdW1 == point2 || x.getIdW2 == point2).collect()

        //Eliminamos los puntos completos
        val matrixSub = matrix.filter(x => !(x.getIdW1 == point1 || x.getIdW2 == point1))
          .filter(x => !(x.getIdW1 == point2 || x.getIdW2 == point2))

        val rddCartesianPoints = sc.parallelize(rddPoints1).cartesian(sc.parallelize(rddPoints2))
        val rddFilteredPoints = rddCartesianPoints.filter(x => (x._1.getIdW2 == x._2.getIdW2) ||
          (x._1.getIdW1 == x._2.getIdW1) ||
          (x._1.getIdW1 == x._2.getIdW2 ||
            (x._2.getIdW1 == x._1.getIdW2)))

        //Se crea un nuevo punto siguiendo la estrategia
        matrix = distanceStrategy match {

          case "min" =>
            //Se calcula la nueva distancia con respecto a todos los puntos y la estrategia escogida
            val newPoints = rddFilteredPoints.map(x => new Distance(newIndex.toInt, filterMatrix(x._1, clustersRes),
              math.min(x._1.getDist, x._2.getDist)))

            //Agrego los puntos con el nuevo indice
            matrixSub.union(newPoints)

          case "max" =>
            //Se calcula la nueva distancia con respecto a todos los puntos y la estrategia escogida
            val newPoints = rddFilteredPoints.map(x => new Distance(newIndex.toInt, filterMatrix(x._1, clustersRes),
              math.max(x._1.getDist, x._2.getDist)))

            matrixSub.union(newPoints)

          case "avg" =>
            //Se calcula la nueva distancia con respecto a todos los puntos y la estrategia escogida
            val newPoints = rddFilteredPoints.map(x => new Distance(newIndex.toInt, filterMatrix(x._1, clustersRes),
              ((x._1.getDist + x._2.getDist) / 2)))

            matrixSub.union(newPoints)
        }
      }

      matrix = matrix.coalesce(partitionNumber / 2).persist(StorageLevel.MEMORY_ONLY_2)

      if (a % 5 == 0) {
        matrix.checkpoint()
      }

      val duration = (System.nanoTime - start) / 1e9d
      println(s"TIME: $duration")
    }

    model
      .map(value => (value._1 - 1, value._2 - 1, value._3, value._4))
      .map(_.toString().replace("(", "").replace(")", ""))
      .coalesce(1, shuffle = true)
      .saveAsTextFile("Points-" + numPoints + "-Clusters-" + numClusters + Utils.whatTimeIsIt())

  }

  //  def runAlgorithmDF(distanceMatrix: DataFrame, numPoints: Int, numPartitions: Int): LinkageModel ={
  //
  //    var matrix = distanceMatrix
  //    val spark = distanceMatrix.sparkSession
  //    import spark.implicits._
  //    val cont = spark.sparkContext.longAccumulator("My Accumulator DF")
  //    cont.add(numPoints)
  //
  //    val linkageModel = new LinkageModel(scala.collection.mutable.Map[Long, Seq[(Int, Int)]]())
  //
  //    for (a <- 0 until (numPoints - numClusters)) {
  //      val start = System.nanoTime
  //
  //      println("Finding minimum:")
  //      val minDistRes = matrix.select(min("dist")).first().getFloat(0)
  //      val clusterRes = matrix.where($"dist" === minDistRes)
  //      println(s"New minimum:")
  //      clusterRes.show(1)
  //
  //      val point1 = clusterRes.first().getInt(0)
  //      val point2 = clusterRes.first().getInt(1)
  //
  //      cont.add(1)
  //      val newIndex = cont.value.toLong
  //
  //      println("New Cluster: " + newIndex + ":" + point1 + "-" + point2)
  //
  //      //Se guarda en el modelo el resultado
  //      linkageModel.getClusters += newIndex -> Seq((point1, point2))
  //
  //      //Si no es el ultimo cluster
  //      if (a < (numPoints - numClusters - 1)) {
  //
  //        //Se elimina el punto encontrado de la matrix original
  ////        val matrixFiltered = matrix.where("!(idW1 == " + point1 +" and idW2 ==" + point2 + " )").repartition(numPartitions).persist()
  //        matrix = matrix.where("!(idW1 == " + point1 +" and idW2 ==" + point2 + " )").cache()
  //        val dfPoints1 = matrix.where("idW1 == " + point1 + " or idW2 == " + point1)
  //
  //        //Renombramos las columnas para poder hacer un filtrado posterior
  //        val newColumnsNames = Seq("distPoints1", "idW1Points1", "idW2Points1")
  //        val dfPoints1Renamed = dfPoints1.toDF(newColumnsNames: _*)
  //
  //        val dfPoints2 = matrix.where("idW1 == " + point2 + " or idW2 == " + point2)
  //
  ////        val dfPoints2Broadcast = spark.sparkContext.broadcast(dfPoints2)
  ////
  ////        val dfUnionPoints = dfPoints1.union(dfPoints2)
  //
  //        val dfCartesianPoints = dfPoints1Renamed.crossJoin(dfPoints2)
  //
  //        val dfFilteredPoints = dfCartesianPoints.filter("(idW1Points1 == idW1) or (idW1Points1 == idW2) " +
  //          "or (idW2Points1 == idW1) or (idW2Points1 == idW2)")
  //        //Elimino los puntos completos
  ////        val matrixSub = matrix.except(dfUnionPoints)
  //        val matrixSub = matrix.where("!(idW1 == " + point1 + " or idW2 == " + point1 + ")")
  //  .where("!(idW1 == " + point2 + " or idW2 == " + point2 + ")")
  //
  //        //Se crea un nuevo punto siguiendo la estrategia
  //        matrix = distanceStrategy match {
  //          case "min" =>
  //            val newPoints = dfFilteredPoints.map(r =>
  //              (newIndex.toInt, filterDF(r.getInt(0),r.getInt(1), point1, point2), math.min(r.getFloat(2),r.getFloat(5))))
  ////            val newPoints = dfPoints1.rdd.map{
  ////              r =>
  ////                val distAux = dfPoints2Broadcast.value.where("idW1 == " + r.getInt(0) + " or idW1 == " + r.getInt(1)
  ////                  + " or idW2 == " + r.getInt(0) + " or idW2 == " + r.getInt(1)).first().getFloat(2)
  ////
  ////                (newIndex.toInt, filterDF(r.getInt(0),r.getInt(1), point1, point2), math.min(r.getFloat(2), distAux))
  ////            }toDF()
  //
  //            //Agrego los puntos con el nuevo indice
  //            val rows = newPoints.toDF().select("_1","_2","_3")
  //            matrixSub.union(rows)
  //
  //            //Borramos los datos de caché de todas las variables persistidas anteriormente
  ////            matrixSub.unpersist()
  //            //            dfPoints2Broadcast.unpersist()
  //            //            dfPoints1.unpersist()
  //
  ////            matrix2
  //
  //        }
  //        matrix.cache()
  //      }
  //
  ////      if (a % 5 == 0)
  ////        matrix.checkpoint()
  //
  //      val duration = (System.nanoTime - start) / 1e9d
  //      println(s"TIME: $duration")
  //    }
  //    linkageModel
  //  }

  def filterMatrix(oldDistance: Distance, clusterReference: Distance): Int = {
    var result = 0

    if (oldDistance.getIdW1 == clusterReference.getIdW1 || oldDistance.getIdW1 == clusterReference.getIdW2) {
      result = oldDistance.getIdW2
    } else if (oldDistance.getIdW2 == clusterReference.getIdW1 || oldDistance.getIdW2 == clusterReference.getIdW2) {
      result = oldDistance.getIdW1
    }

    result
  }

  def filterDF(idW1: Int, idW2: Int, pointReference1: Int, pointReference2: Int): Int = {
    var result = idW1

    if (idW1 == pointReference1 || idW1 == pointReference2) {
      result = idW2
    }

    result
  }
}

object Linkage {

  //Return the distance between two given clusters
  def clusterDistance(
                       c1: Cluster,
                       c2: Cluster,
                       distanceMatrix: scala.collection.Map[(Int, Int), Float],
                       strategy: String): Double = {
    var res = 0.0
    var aux = res

    strategy match {
      case "min" =>
        res = 100.0 //QUESTION: No se podría poner otro valor ?

        c1.getCoordinates.foreach { x =>
          c2.getCoordinates.foreach { y =>
            //Look for just in the upper diagonal of the "matrix"
            if (x < y) {
              aux = distanceMatrix(x, y)
            }
            else {
              aux = distanceMatrix(y, x)
            }
            if (aux < res)
              res = aux

          }
        }


      case "max" =>
        res = 0.0
        c1.getCoordinates.foreach { x =>
          c2.getCoordinates.foreach { y =>
            //Look for just in the upper diagonal of the "matrix"
            if (x < y) {
              aux = distanceMatrix(x, y)
            } else {
              aux = distanceMatrix(y, x)
            }
            if (aux > res)
              res = aux
          }
        }

      case "avg" =>


    }

    res

  }


  //Calculate the distance between two vectors
  //DEPRECATED
  private def calculateDistance(
                                 v1: Vector,
                                 v2: Vector,
                                 strategy: String): Double = {
    var totalDist = 0.0
    for (z <- 1 to v1.size) {
      var minAux = 0.0
      try {
        val line = v1.apply(z)
        val linePlus = v2.apply(z)
        //El mínimo se suma a totalDist
        if (line < linePlus) {
          minAux = line
        } else {
          minAux = linePlus
        }
      } catch {
        case e: Exception => null
      } finally {
        totalDist += minAux
      }

    }
    totalDist

  }
}