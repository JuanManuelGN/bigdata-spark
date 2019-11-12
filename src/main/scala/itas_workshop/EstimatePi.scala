package itas_workshop

import scala.math.random
import org.apache.spark.{SparkConf, SparkContext}

class EstimatePi {
  def estimatePi(args:String*) = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("EstimatePi")
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = 100000 * slices
    val count = spark.parallelize(1 to n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
  }
}

object EstimatePi extends App {
  val pi = new EstimatePi
  pi.estimatePi("1000")
}