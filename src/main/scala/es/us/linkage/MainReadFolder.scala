package es.us.linkage

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MainReadFolder {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Linkage")
    .setMaster("local[*]")
    //.set("spark.driver.parallelism", "8")
    //.set("spark.driver.maxResultSize", "0")
    //.set("spark.executor.heartbeatInterval", "3000s")

    val sc = new SparkContext(conf)

    sc.setCheckpointDir("checkpoints")

    //val fileTest = "src/main/resources/Distances_full_dataset"
    val fileTest = "src/main/resources/distanceTest"

    var origen: String = fileTest
    var destino: String = Utils.whatTimeIsIt()
    var numPartitions = 16 // cluster has 25 nodes with 4 cores. You therefore need 4 x 25 = 100 partitions.
    var numPoints = 9
    var numClusters = 1
    var strategyDistance = "min"

    if (args.length > 2) {
      origen = args(0)
      destino = args(1)
      numPartitions = args(2).toInt
      numPoints = args(3).toInt
      numClusters = args(4).toInt
      strategyDistance = args(5)
    }


    //val partCustom = new HashPartitioner(numPartitions)

    val distances = sc.textFile(origen, numPartitions)
      .map(s => s.split(',').map(_.toFloat))
      .map { case x =>
        new Distance(x(0).toInt, x(1).toInt, x(2))
      }.filter(x => x.getIdW1 < x.getIdW2)

    val data = sc.parallelize(Cluster.createInitClusters(numPoints))
    println("Number of points: " + data.count())

    //min,max,avg
    val linkage = new Linkage(numClusters, strategyDistance)
    println("New Linkage with strategy: " + strategyDistance)

    val model = linkage.runAlgorithm(distances, numPoints)

    println("RESULT: ")
    model.printSchema(";")

    sc.parallelize(model.saveSchema).coalesce(1, shuffle = true).saveAsTextFile(destino + "Linkage-" + Utils.whatTimeIsIt())

    sc.stop()
  }
}