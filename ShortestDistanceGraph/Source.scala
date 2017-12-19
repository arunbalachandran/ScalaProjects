// Arun Balchandran 1001402679

package edu.uta.cse6331

import org.apache.spark.{SparkConf, SparkContext}

object Source {
  val numNodes = 100001               // hardcoded
  val maxDistanceValue = 999999       // hardcoded
  val zeroPath = Array.tabulate(numNodes)(_ => maxDistanceValue)
  zeroPath(0) = 0
  val outputMap = scala.collection.mutable.Map[Int, Int]()
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("SingleShortestDistance")
    sparkConf.setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    val graphFile = sparkContext.textFile(args(0))
    val firstMapperOutput = graphFile.map(line => {
      val lineSplit = line.split(",")
      // this will be the returned value
      val i = lineSplit(0).toInt
      val d = lineSplit(1).toInt
      val j = lineSplit(2).toInt
      (j, i, d)
    })

    for (i <- 1 to 4) {
      firstMapperOutput.foreach((dataElement) => {
        if (zeroPath(dataElement._1) > zeroPath(dataElement._2) + dataElement._3) {
          val sum_val = zeroPath(dataElement._2) + dataElement._3
          zeroPath(dataElement._1) = sum_val
        }
      })
    }

    for (i <- 0 until zeroPath.length) {
      if (zeroPath(i) != maxDistanceValue) {
        outputMap(i) = zeroPath(i)
        println(i + " " + zeroPath(i))
      }
    }

    val RDDMap = sparkContext.parallelize(outputMap.toSeq)
    val RDDMapOut = RDDMap.groupByKey().map({case(key, values) => {
        (key + " " + (values.toList)(0))
      }
    })

    RDDMapOut.coalesce(1).saveAsTextFile(args(1))
  }
}
