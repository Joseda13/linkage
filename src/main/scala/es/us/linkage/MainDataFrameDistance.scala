package es.us.linkage

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id

/**
  * Created by Jose David on 8/03/2018.
  */

object MainDataFrameDistance {
  def main(args: Array[String]): Unit = {

    val start = System.nanoTime

    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("DistancesPoints")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val fileTest = "B:\\Datasets\\irisData.txt"
//    val fileTest = "B:\\Datasets\\glassData_10.txt"

    var origen: String = fileTest
    var dataName: String = "iris"
    var numPartitions = 16 // cluster has 25 nodes with 4 cores. You therefore need 4 x 25 = 100 partitions.
    var typDataSet = 1
    var idIndex = "_c0"
    var classIndex = "_c4"
    var distanceMethod = "Euclidean"

    if (args.length > 2) {
      origen = args(0)
      dataName = args(1)
      numPartitions = args(2).toInt
      typDataSet = args(3).toInt
      idIndex = args(4)
      classIndex = args(5)
      distanceMethod = args(6)
    }

    println("DataBase: " + dataName)
    println("Distance Method: " + distanceMethod)

    println("---Reading the data---")

    //Load data from csv
    val dataDF = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .csv(origen)

    println("---Data read---")

    println("---Filtering data---")

    //Filtering DataFrame
    val dataDFFiltered = typDataSet match{
      //It is not necessary to remove any column
      case 0 =>
        dataDF.map(_.toSeq.asInstanceOf[Seq[Double]])
      //It is necessary to delete the class column
      case 1 =>
        dataDF.drop(classIndex).map(_.toSeq.asInstanceOf[Seq[Double]])
      //It is necessary to eliminate both the identifier and the class
      case 2 =>
        dataDF.drop(idIndex,classIndex).map(_.toSeq.asInstanceOf[Seq[Double]])
    }
dataDFFiltered.rdd.coalesce(1, shuffle = true)
  .saveAsTextFile(dataName + "Points")
    println("---Data filtered---")

    //We automatically generate an index for each row
    val dataAux = dataDFFiltered.withColumn("index", monotonically_increasing_id()+1)

    //Save dataAux for futures uses
    dataAux.map(row => (row.getLong(1), row.getSeq[Double](0).toList))
      .rdd
      .sortByKey()
      .map(_.toString().replace("(", "").replace("))", ")").replace("List", "(").replace(",(", ";("))
      .coalesce(1, shuffle = true)
      .saveAsTextFile(dataName + "Coordinates")

    //Rename the columns and generate a new DataFrame copy of the previous to be able to do the subsequent filtered out in the join
    val newColumnsNames = Seq("valueAux", "indexAux")
    val dataAuxRenamed = dataAux.toDF(newColumnsNames: _*)

    println("---Calculating distances---")

    val distances = dataAux.crossJoin(dataAuxRenamed)
      .filter(r => r.getLong(1) < r.getLong(3))
      .map{r =>
        //Depending on the method we choose to perform the distance, the value of the same will change
        val dist = distanceMethod match {

          case "Euclidean" =>
            distEuclidean(r.getSeq[Double](0), r.getSeq[Double](2))
        }

        //We return the result saving: (point 1, point 2, the distance that separates both)
        (r.getLong(1), r.getLong(3), dist)
      }

    println("---Distances calculated---")

    println("---Saving file---")

    //We save in a file the result of the distances of all the points in the database
    distances.rdd.map(_.toString().replace("(", "").replace(")", ""))
      .coalesce(1, shuffle = true)
      .saveAsTextFile(dataName + "Distances")

    println("---File saved---")

    val duration = (System.nanoTime - start) / 1e9d
    println(s"TIME TOTAL: $duration")

    spark.stop()
  }

  //Method for calculating de Euclidean distante between two points
  def distEuclidean(v1: Seq[Double], v2: Seq[Double]): Double = {
    require(v1.size == v2.size, s"Vector dimensions do not match: Dim(v1)=${v1.size} and Dim(v2)" +
      s"=${v2.size}.")
    var squaredDistance = 0.0

    var kv = 0
    val sz = v1.size
    while (kv < sz) {
      val score = v1.apply(kv) - v2.apply(kv)
      squaredDistance += Math.abs(score*score)
      kv += 1
    }
    math.sqrt(squaredDistance)
  }
}
