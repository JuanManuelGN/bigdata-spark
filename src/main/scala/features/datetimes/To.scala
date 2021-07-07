package features.datetimes

import config.SparkConfig
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.to_utc_timestamp

trait To {
  /**
    * 2012-04-10T10:45:13.815-01:00 -> 2012-04-10 11:45:13.815
    *            ^^                               ^^
    * @return
    */
  def stringTimestampWithTimezoneToUtcTimestamp: Column => Column = c =>
    to_utc_timestamp(c, "Europe/Berlin")
}

object ToApp extends To with App with SparkConfig {

  import spark.implicits._

  val df: DataFrame =
    List(
      "2012-04-10T10:45:13.815-01:00"
    ).toDF()

  df.withColumn(
    "utc",
    stringTimestampWithTimezoneToUtcTimestamp(df(df.columns.head))
  ).show(false)
}
