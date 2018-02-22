package es.us.linkage

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object MainReadFolder {
  def main(args: Array[String]): Unit = {
    val start = System.nanoTime
    Logger.getLogger("org").setLevel(Level.OFF)

    val conf = new SparkConf()
      .setAppName("Linkage")
      .setMaster("local[*]")
//        .set("spark.serializer", "KryoSerializer")
//      .set("spark.kryo.registrationRequired", "true")
//      .registerKryoClasses(Array(classOf[Distance]))
//      .set("spark.files.fetchTimeout", "5min")
//      .set("spark.memory.fraction", "0.7")
//      .set("spark.reducer.maxSizeInFlight", "72mb")
//      .set("spark.shuffle.file.buffer", "48k")
//      .set("spark.rdd.compress", "true")

    val sc = new SparkContext(conf)

    sc.setCheckpointDir("B:\\checkpoints")
//    val fileTest = "B:\\Datasets\\Distances_full_dataset"
    val fileTest = "B:\\Datasets\\distanceTest"

//    val  fileTest = ""

    var origen: String = fileTest
    var destino: String = Utils.whatTimeIsIt()
    var numPartitions = 16 // cluster has 25 nodes with 4 cores. You therefore need 4 x 25 = 100 partitions.
    var numPoints = 9
    var numClusters = 1
    var strategyDistance = "avg"

    if (args.length > 2) {
      origen = args(0)
      destino = args(1)
      numPartitions = args(2).toInt
      numPoints = args(3).toInt
      numClusters = args(4).toInt
      strategyDistance = args(5)
    }

    val distances = sc.textFile(origen, numPartitions)
      .map(s => s.split(',').map(_.toFloat))
      .map { case x =>
        new Distance(x(0).toInt, x(1).toInt, x(2))
      }.filter(x => x.getIdW1 < x.getIdW2).repartition(numPartitions)

    val data = sc.parallelize(Cluster.createInitClusters(numPoints))
    println("Number of points: " + data.count())

    //min,max,avg
    val linkage = new Linkage(numClusters, strategyDistance)
    println("New Linkage with strategy: " + strategyDistance)

    val model = linkage.runAlgorithm(distances, numPoints)

    println("RESULT: ")
    model.printSchema(";")

    model.saveSchema(destino)

    val duration = (System.nanoTime - start) / 1e9d
    println(s"TIME TOTAL: $duration")

    sc.stop()
  }
}