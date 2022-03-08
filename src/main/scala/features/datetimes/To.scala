package features.datetimes

import config.SparkConfig
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, date_format, from_utc_timestamp, hour, substring, to_timestamp, to_utc_timestamp, unix_timestamp}
import org.apache.spark.sql.types.TimestampType

trait To {
  /**
    * 2012-04-10T10:45:13.815-01:00 -> 2012-04-10 11:45:13.815
    *            ^^                               ^^
    * @return
    */
  def stringTimestampWithTimezoneToUtcTimestamp: Column => Column = c =>
    to_utc_timestamp(c, "Europe/Berlin").cast(TimestampType)
}

object ToApp extends To with App with SparkConfig {

  import spark.implicits._

  val df: DataFrame =
    List(
      "2012-04-10T10:45:13.815-01:00",
      "2020-12-09T01:50:24.815Z",
      "2012-04-10 10:45:13"
    ).toDF("date")

  df
    .withColumn("utc", to_utc_timestamp(df("date"), "Europe/Berlin").cast(TimestampType))
    .withColumn("utc_paris", to_utc_timestamp(df("date"), "Europe/Paris").cast(TimestampType))
    .withColumn("unix_timestamp", unix_timestamp(df("date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").cast(TimestampType))
    .withColumn("to_timestamp", to_timestamp(df("date")).cast(TimestampType))
    .withColumn("from_utc_timestamp", from_utc_timestamp(df("date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").cast(TimestampType))
    .withColumn("hour_from_utc_timestamp", hour(col("from_utc_timestamp")))
    .withColumn("utc_to_yyyy-MM-dd-HH:mm:ss", date_format(col("utc"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("hour_utc", hour(col("utc")))
    .withColumn("hour_utc_paris", hour(col("utc_paris")))
    .withColumn("hour_unix_timestamp", hour(col("unix_timestamp")))
    .withColumn("hour_to_timestamp", hour(col("to_timestamp")))
    .withColumn("hour_from_string", substring(col("date"), 13, 2))
    .show(false)
}
