package itas_workshop

import config.SparkConfig
import org.apache.spark.rdd.RDD

class MapVsFlatMap extends SparkConfig {
  def loadFile: RDD[String] = {
    val quijote = "data/cervantes.txt"
    spark.sparkContext.textFile(quijote)
  }

  /**
    *
    * @param rdd
    * @return
    *         ((EL), (INGENIOSO), (HIDALGO), (DON), (QUIJOTE), (DE), (LA), (MANCHA)),
    *         (()),
    *         ((Miguel), (de), (Cervantes), (Saavedra)),
    *         .
    *         .
    *         .
    *
    */
  def doMap(rdd: RDD[String]) = rdd.map(_.split(" ")).take(10)

  /**
    *
    * @param rdd
    * @return
    *         (EL, INGENIOSO, HIDALGO, DON, QUIJOTE, DE, LA, MANCHA, , Miguel,...)
    *
    */
  def doFlatMap(rdd: RDD[String]) = rdd.flatMap(_.split(" ")).take(10)
}

object MapVsFlatMap extends App {
  val mvsf = new MapVsFlatMap
  val rdd =  mvsf.loadFile
  mvsf.doMap(rdd).foreach(l => l.foreach(println(_)))
  mvsf.doFlatMap(rdd).foreach(println(_))
}
