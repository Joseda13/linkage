package es.us.linkage

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
  * Created by Jose David on 20/02/2018.
  */

object MainCreateCluster {
  def main(args: Array[String]): Unit = {
    val start = System.nanoTime
    Logger.getLogger("org").setLevel(Level.OFF)

    val conf = new SparkConf()
      .setAppName("Clustering")
      .setMaster("local[*]")
      .set("spark.files.fetchTimeout", "10min")

    val sc = new SparkContext(conf)

    sc.setCheckpointDir("B:\\checkpoints")

//        val fileTest = "C:\\Users\\Jose David\\IdeaProjects\\linkage\\Linkage-EMPLEO-FULL(AVG)\\part-00000"
//    val fileTest = "C:\\Users\\Jose David\\IdeaProjects\\linkage\\201802190936Linkage-201802190936\\part-00000"
    val fileTest = "C:\\Users\\Jose David\\IdeaProjects\\linkage\\Linkage-IRIS-FULL(AVG)\\part-00000"
//    val fileTest = "C:\\Users\\Jose David\\IdeaProjects\\linkage\\Linkage-GLASS-FULL(AVG)\\part-00000"
//    val fileTest = "C:\\Users\\Jose David\\IdeaProjects\\linkage\\201803091138Linkage-201803091138\\part-00000"
//    val fileTest = ""

    var origen: String = fileTest
    var destino: String = Utils.whatTimeIsIt()
    var numPartitions = 16 // cluster has 25 nodes with 4 cores. You therefore need 4 x 25 = 100 partitions.
    var numPoints = 150
    var numClusters = 3

    if (args.length > 2) {
      origen = args(0)
      destino = args(1)
      numPartitions = args(2).toInt
      numPoints = args(3).toInt
      numClusters = args(4).toInt
    }

    val clusters = sc.textFile(origen)
      .map(s => s.split(',').map(_.toInt))
      .map{
        case x => (x(0).toLong, (x(1), x(2)))
      }

    //Initialize an RDD from 1 to the number of points in our database
    val totalPoints = sc.parallelize(1 to numPoints).cache()

    //We create a model based on the clustering established in the source file
    val model = new LinkageModel(clusters,sc.emptyRDD[Vector].collect())

    val orig = "C:\\Users\\Jose David\\IdeaProjects\\linkage\\irisCoordinates\\irisPoints"

    val coordinates = sc.textFile(orig)
      .map(s => s.split(";"))
      .map(row => (row(0).toInt, Vectors.dense(row(1).replace("(", "").replace(")", "").split(",").map(_.toDouble))))

    val resultPoints = model.createClusters(destino, numPoints, numClusters, totalPoints)
    val centroids = model.inicializeCenters(coordinates, numClusters, numPoints, resultPoints)

    model.setClusterCenters(centroids)

    try {

      val testComputeCost = model.computeCost(coordinates.map(point => point._2))

      val testPredict = model.predict(coordinates.first()._2)

//      println(testComputeCost)

//      model.saveResult(destino, clusters, numPoints, numClusters)
      println("OK")
    }
    catch {
      case _: Throwable => println("FAIL")
    }

    val duration = (System.nanoTime - start) / 1e9d
    println(s"TIME TOTAL: $duration")

    sc.stop()
  }

}
