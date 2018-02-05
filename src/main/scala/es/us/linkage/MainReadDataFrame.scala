package es.us.linkage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object MainReadDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("LinkageDF")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setCheckpointDir("checkpoints")
//    spark.sqlContext.setConf("spark.sql.orc.filterPushdown", "true")
//    spark.sqlContext.setConf("spark.sql.orc.filterPushdown", "true")

    val fileTest = "src/main/resources/distanceTest"
//    val fileTest = "src/main/resources/Distances_full_dataset"

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

    //Load data from csv
    val distancesDF = spark.read
      .option("header", "false")
      .option("inferSchema", "false")
      .option("delimiter", ",")
      .csv(origen)

    //Casting
    val distancesDFAux = distancesDF.select(
      distancesDF("_c0").cast(IntegerType).as("idW1"),
      distancesDF("_c1").cast(IntegerType).as("idW2"),
      distancesDF("_c2").cast(FloatType).as("dist")
    ).filter("idW1 < idW2")

    println("Number of points: " + numPoints)
    //min,max,avg
    val linkage = new Linkage(numClusters, strategyDistance)
    println("New Linkage with strategy: " + strategyDistance)

    val model = linkage.runAlgorithmDF(distancesDFAux, numPoints, numPartitions)

    println("RESULT: ")
    model.printSchema(";")

    spark.sparkContext.parallelize(model.saveSchema).coalesce(1, shuffle = true).saveAsTextFile(destino + "LinkageDF-" + Utils.whatTimeIsIt())

    spark.stop()
  }
}
