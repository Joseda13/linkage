package es.us.linkage

import org.apache.spark.sql.functions.min
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.UDFRegistration
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
  * Created by Josem on 15/11/2016.
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
    val cont = sc.longAccumulator("My Accumulator")
    cont.add(numPoints)
    val partitionNumber = distanceMatrix.getNumPartitions

    val linkageModel = new LinkageModel(scala.collection.mutable.Map[Long, Seq[(Int, Int)]]())

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

      //Se guarda en el modelo resultado
      linkageModel.getClusters += newIndex -> Seq((point1, point2))

      //Si no es el ultimo cluster
      if (a < (numPoints - numClusters - 1)) {

        //Se elimina el punto encontrado
        matrix = matrix.filter(x => !(x.getIdW1 == point1 && x.getIdW2 == point2)).repartition(partitionNumber).cache()

        val rddPoints1 = matrix.filter(x => x.getIdW1 == point1 || x.getIdW2 == point1).repartition(partitionNumber).cache()
        val rddPoints2 = matrix.filter(x => x.getIdW1 == point2 || x.getIdW2 == point2).repartition(partitionNumber).cache()
        val rddUnionPoints = rddPoints1.union(rddPoints2)

        val rddCartesianPoints = rddPoints1.cartesian(rddPoints2)
        val rddFilteredPoints = rddCartesianPoints.filter(x => (x._1.getIdW2 == x._2.getIdW2) ||
          (x._1.getIdW1 == x._2.getIdW1) ||
          (x._1.getIdW1 == x._2.getIdW2 ||
            (x._2.getIdW1 == x._1.getIdW2))).repartition(partitionNumber)
        rddPoints1.unpersist()
        rddPoints2.unpersist()

        //Se crea un nuevo punto siguiendo la estrategia
        matrix = distanceStrategy match {
          case "min" =>
            val newPoints = rddFilteredPoints.map(x => new Distance(newIndex.toInt, filterMatrix(x._1,clustersRes), math.min(x._1.getDist,x._2.getDist)))
              .repartition(partitionNumber)

            //Elimino los puntos completos
            val matrixSub = matrix.subtract(rddUnionPoints).cache()

            //Agrego los puntos con el nuevo indice
            matrix = matrixSub.union(newPoints)

            //val matrixSub2 = matrixSub.filter(x => x.getIdW2 == punto1 || x.getIdW2 == punto2).cache()
            matrixSub.unpersist()

            /*if (matrixSub2.countApprox(3000) != 0){
              val matrixCartesian = matrixSub2.cartesian(matrixSub2)
                .filter(x => x._1.getIdW1 == x._2.getIdW1 && x._1.getIdW2 < x._2.getIdW2)
              val editedPoints = matrixCartesian.map(x => new Distance(x._1.getIdW1, newIndex.toInt, math.min(x._1.getDist, x._2.getDist)))
              matrix = matrix.subtract(matrixSub2)
                .union(editedPoints)
              matrixSub2.unpersist()
            }*/
            matrix

          case "max" =>
            //Calcula distancia
            val newPoints = rddFilteredPoints.map(x => new Distance(newIndex.toInt, filterMatrix(x._1,clustersRes), math.max(x._1.getDist,x._2.getDist)))

            //Elimino los puntos completos
            val matrixSub = matrix.subtract(rddUnionPoints)

            //agrego puntos con el nuevo indice
            matrix = matrixSub.union(newPoints)

            val matrixSub2 = matrixSub.filter(x => x.getIdW2 == point1 || x.getIdW2 == point2).repartition(partitionNumber).cache()

            if (matrixSub2.count() > 0) {
              val matrixCartesian = matrixSub2.cartesian(matrixSub2)
                .filter(x => x._1.getIdW1 == x._2.getIdW1 && x._1.getIdW2 < x._2.getIdW2)

              val editedPoints = matrixCartesian.map(x => new Distance(x._1.getIdW1, newIndex.toInt, math.min(x._1.getDist, x._2.getDist)))

              matrix = matrix.subtract(matrixSub2).union(editedPoints).repartition(partitionNumber)
            }
            matrix
        }
        matrix.cache()
      }
      if (a % 5 == 0)
        matrix.checkpoint()

      val duration = (System.nanoTime - start) / 1e9d
      println(s"TIME: $duration")

    }
    linkageModel
  }

  def runAlgorithmDF(distanceMatrix: DataFrame, numPoints: Int, numPartitions: Int): LinkageModel ={

    var matrix = distanceMatrix
    val spark = distanceMatrix.sparkSession
    import spark.implicits._
    val cont = spark.sparkContext.longAccumulator("My Accumulator DF")
    cont.add(numPoints)

    val linkageModel = new LinkageModel(scala.collection.mutable.Map[Long, Seq[(Int, Int)]]())

    for (a <- 0 until (numPoints - numClusters)) {
      val start = System.nanoTime

      println("Finding minimum:")
      val minDistRes = matrix.select(min("dist")).first().getFloat(0)
      val clusterRes = matrix.where($"dist" === minDistRes)
      println(s"New minimum:")
      clusterRes.show(1)

      val point1 = clusterRes.first().getInt(0)
      val point2 = clusterRes.first().getInt(1)

      cont.add(1)
      val newIndex = cont.value.toLong

      println("New Cluster: " + newIndex + ":" + point1 + "-" + point2)

      //Se guarda en el modelo el resultado
      linkageModel.getClusters += newIndex -> Seq((point1, point2))

      //Si no es el ultimo cluster
      if (a < (numPoints - numClusters - 1)) {

        //Se elimina el punto encontrado de la matrix original
//        val matrixFiltered = matrix.where("!(idW1 == " + point1 +" and idW2 ==" + point2 + " )").repartition(numPartitions).persist()
        matrix = matrix.where("!(idW1 == " + point1 +" and idW2 ==" + point2 + " )").cache()
        val dfPoints1 = matrix.where("idW1 == " + point1 + " or idW2 == " + point1).cache()

//        //Renombramos las columnas para poder hacer un filtrado posterior
//        val newColumnsNames = Seq("distPoints1", "idW1Points1", "idW2Points1")
//        val dfPoints1Renamed = dfPoints1.toDF(newColumnsNames: _*)
9
        val dfPoints2 = matrix.where("idW1 == " + point2 + " or idW2 == " + point2).cache()

        val dfPoints2Broadcast = spark.sparkContext.broadcast(dfPoints2)

        val dfUnionPoints = dfPoints1.union(dfPoints2).cache()

//        val dfCartesianPoints = dfPoints1Renamed.crossJoin(dfPoints2)

//        val dfFilteredPoints = dfCartesianPoints.filter("(idW1Points1 == idW1) or (idW1Points1 == idW2) " +
//          "or (idW2Points1 == idW1) or (idW2Points1 == idW2)")
        //Elimino los puntos completos
        val matrixSub = matrix.except(dfUnionPoints).cache()

        //Se crea un nuevo punto siguiendo la estrategia
        matrix = distanceStrategy match {
          case "min" =>
//            val newPoints = dfFilteredPoints.map(r =>
//              (newIndex.toInt, filterDF(r.getInt(0),r.getInt(1), point1, point2), math.min(r.getFloat(2),r.getFloat(5))))
            val newPoints = dfPoints1.map{
              r =>
                val distAux = dfPoints2Broadcast.value.where("idW1 == " + r.getInt(0) + " or idW1 == " + r.getInt(1)
                  + " or idW2 == " + r.getInt(0) + " or idW2 == " + r.getInt(1)).first().getFloat(2)

                (newIndex.toInt, filterDF(r.getInt(0),r.getInt(1), point1, point2), math.min(r.getFloat(2), distAux))
            }.asInstanceOf[Dataset[Row]]

            //Agrego los puntos con el nuevo indice
//            val rows = newPoints.toDF().select("_1","_2","_3")
            matrixSub.union(newPoints)

            //Borramos los datos de caché de todas las variables persistidas anteriormente
//            matrixSub.unpersist()
            //            dfPoints2Broadcast.unpersist()
            //            dfPoints1.unpersist()

//            matrix2

        }
        matrix.cache()
      }

      if (a % 5 == 0)
        matrix.checkpoint()

      val duration = (System.nanoTime - start) / 1e9d
      println(s"TIME: $duration")
    }
    linkageModel
  }

  def filterMatrix(oldDistance: Distance, clusterReference: Distance): Int = {
    var result = 0

    if (oldDistance.getIdW1 == clusterReference.getIdW1 || oldDistance.getIdW1 == clusterReference.getIdW2){
      result = oldDistance.getIdW2
    }else if (oldDistance.getIdW2 == clusterReference.getIdW1 || oldDistance.getIdW2 == clusterReference.getIdW2){
      result = oldDistance.getIdW1
    }

    result
  }

  def filterDF(idW1: Int, idW2: Int, pointReference1: Int, pointReference2: Int): Int = {
    var result = idW1

    if (idW1 == pointReference1 || idW1 == pointReference2){
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