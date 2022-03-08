package config

import org.apache.spark.sql.SparkSession

trait SparkConfig {

  implicit lazy val spark: SparkSession =
    SparkSession.builder()
      .appName("jeje")
      .config("spark.master", "local")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
}
