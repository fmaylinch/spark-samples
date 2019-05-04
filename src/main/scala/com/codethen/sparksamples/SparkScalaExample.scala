package com.codethen.sparksamples

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkScalaExample {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("Example")
      .config("spark.master", "local")
      .getOrCreate

    val sc: SparkContext = spark.sparkContext

    val data = sc.parallelize(List(9, 2, 3, 5, 6, 7, 2, 1))

    val newData = data
      .filter{ _ % 2 == 0 }
      .map{ _ - 1 }

    println("Collected values: " + newData.collect().toList)
    println("Sum: " + newData.reduce(_ + _))
  }
}
