package features

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}

class TimeTransformer {
  /**
    * Transformación de una columna con un tipo de fecha, unix, en otra, yyyymmdd
    * @param unixDf DataFrame con una columna que representa una fecha cualquiera en formato Unix, es
    *               decir, es un long con los milisegundos que han pasado desde 1970
    * @return DaTaFrame de entrada con una columna añadida con la fecha formateada en yyyymmdd en
    *         formato integer
    */
  def unixToYyyymmdd(unixDf: DataFrame): DataFrame = {
    val unixColumnName = unixDf.columns.head
    val columnName = "YYYYMMDD"
    val format = "yyyyMMdd"
    unixDf
      .withColumn(columnName, from_unixtime(col(unixColumnName) / 1000, format).cast(IntegerType))
  }

  /**
    *
    * @param unixDf dataframe con una columna de tiempo en formato unix (milisegundos)
    * @return
    */
  def unixToDate(unixDf: DataFrame): DataFrame = {
    val unixColumnName = unixDf.columns.head
    val format = "yyyyMMdd"

    unixDf
      .withColumn("DATE", to_date(from_unixtime(col(unixColumnName)/1000), "yyyy-MM-dd"))
//      .withColumn("a", from_unixtime(col(unixColumnName) / 1000, format).cast(IntegerType))
//        .withColumn("c", to_date(col("a").cast(StringType), format))
//      .select(to_date(col(unixColumnName).cast(StringType), format).as("Date"))
  }
  def unixSecondsToDate(unixDf: DataFrame): DataFrame = {
    val unixColumnName = unixDf.columns.head
    val format = "yyyy-MM-dd"

    unixDf
      .withColumn("DATE", to_date(from_unixtime(col(unixColumnName)), format))
  }

  def YyyyMmDdToLong(df: DataFrame): DataFrame = {
    val inputColumnName = "timeIntFormat"
    val inputColumnFormat = "yyyyMMdd"
    val outputColumnName = "TimeFormatted"
    df.withColumn(outputColumnName,
      unix_timestamp(col(inputColumnName).cast(StringType), inputColumnFormat))
  }

  def intToDate(df: DataFrame): DataFrame = {
    val inputColumnName = "timeIntFormat"
    val inputColumnFormat = "yyyyMMdd"
    val outputColumnName = "TimeFormatted"

//    df.withColumn(outputColumnName, to_date(col(inputColumnName).cast(StringType), inputColumnFormat))
    df.withColumn("old", to_date(lit("10000101"), inputColumnFormat))
      .select(to_date(col(inputColumnName).cast(StringType), inputColumnFormat).as("TimeFormatted"),
              col("old"))
  }

}
object TimeTransformer extends App {
  val timeTransformer = new TimeTransformer

//  val timeDf = CreateDataframe.getTimeUnixDf
//  val timeYyyyMmDd = timeTransformer.unixToYyyymmdd(timeDf)
//  timeDf.show
//  timeYyyyMmDd.show

  /**
    * long(1573649887000) in miliseconds to date(yyyy-MM-dd)
    */
//  val timeLongDf = CreateDataframe.getTimeUnixDf
//  val timeLongToDateDf = timeTransformer.unixToDate(timeLongDf)
//  timeLongDf.show
//
//  timeLongToDateDf.printSchema
//  timeLongToDateDf.show
  /*************************************************************/
  /**
    * long(1573649887) in seconds to date(yyyy-MM-dd)
    */
//  val timeUnixSecondsLongDf = CreateDataframe.getTimeUnixSecondsDf
//  val timeUnixSecondsToDateDf = timeTransformer.unixSecondsToDate(timeUnixSecondsLongDf)
//
//  timeUnixSecondsLongDf.show
//  timeUnixSecondsToDateDf.printSchema
//  timeUnixSecondsToDateDf.show
  /*************************************************************/

//  timeTransformer.timeUnixDf.printSchema
//  timeYyyyMmDd.printSchema
//  timeTransformer.timeUnixDf.show
//  timeYyyyMmDd.show

  val timeIntegerFormatDf = CreateDataframe.getTimeIntegerFormatDf
  val timeIntegerFormatDf2 = CreateDataframe.getTimeIntegerFormatDf2
  val timeIntegerFormatted = timeTransformer.YyyyMmDdToLong(timeIntegerFormatDf)
  val timeIntegerFormatted2 = timeTransformer.YyyyMmDdToLong(timeIntegerFormatDf2).withColumnRenamed("TimeFormatted", "TimeFormatted2")
  timeIntegerFormatted.show
  timeIntegerFormatted2.show
//
//  val dfTime =
//    timeIntegerFormatted2
//      .join(timeIntegerFormatted, Seq("id"))
//      .withColumn("Minus", col("TimeFormatted2") - col("TimeFormatted"))
//
//  dfTime.show
}

class NumberTransformer {
  val numberDf = CreateDataframe.getNumericalDf

  def integerToLong(df: DataFrame): DataFrame = {
    df.withColumn("long", col("integer").cast(LongType))
  }
}
object NumberTransformer extends App {
  val numberTransformer = new NumberTransformer()
  val integerDf = CreateDataframe.getNumericalDf
  val longDf = numberTransformer.integerToLong(integerDf)

  integerDf.printSchema
  longDf.printSchema

  integerDf.show
  longDf.show
}

object IntToDate extends App {
  val df: DataFrame = CreateDataframe.getTimeIntegerFormatDf
  df.printSchema
  df.show

  val dfTransformed = new TimeTransformer().intToDate(df)
  dfTransformed.printSchema
  dfTransformed.show
}