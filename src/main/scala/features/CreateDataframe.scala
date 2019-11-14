package features

import config.SparkConfig
import features.AddColumnToDF.spark
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

/**
  * En esta clase está centralizada la creación de los dataframes que el resto de componentes van
  * a necesitar para su ejecución
  */
case class CreateDataframe() extends SparkConfig {

  /**
    * Creación de un dataframe a partir de una secuencia de Rows. Para la creación es necesario
    * crear un RDD para luego crear el DF indicando la estructura de las columnas del mismo
    */
  private val card =
    Seq(
      Row("506", "013", "2018"),
      Row("506", "016", "2017"),
      Row("506", "014", "2017"),
      Row("506", "017", "2017"),
      Row("506", "015", "2017"),
      Row("506", "0018", "2017"),
      Row("0", "013", "2017"),
      Row("506", "10", "2017"),
      Row("5", "8", "2017")
    )
  private val cardRDD = spark.sparkContext.parallelize(card)

  private val cardDF =
    spark.createDataFrame(cardRDD, StructType(Seq(
                                    StructField("tipo_tarj", StringType, true),
                                    StructField("subtipo_tarj", StringType, true),
                                    StructField("load_date", StringType, true))))

  /**
    *
    */
  import spark.implicits._

  private val df1 =
    Seq(1560, 1560, 1560, 1560, 1561, 1561, 1561, 1561, 1562, 1562, 1563)
      .toDF("c1")
  private val df2 =
    Seq(1560, 1560, 1560, 1560, 1561, 1561, 1561, 1561, 1562, 1562, 1563)
      .toDF("c2")

  private val timeSchema = StructType(List(StructField("unix", LongType)))
  private val timeRaw = List(Row(1435655706000L))
  private val timeUnixDf = spark.createDataFrame(spark.sparkContext.parallelize(timeRaw), timeSchema)

  private val timeUnixSecondsSchema = StructType(List(StructField("unixSeconds", LongType)))
  private val timeUnixSecondsRaw = List(Row(1435655706L))
  private val timeUnixSecondsDf =
    spark.createDataFrame(spark.sparkContext.parallelize(timeUnixSecondsRaw), timeUnixSecondsSchema)

  private val timeIntegerSchema = StructType(List(StructField("id", IntegerType),
                                                  StructField("timeIntFormat", IntegerType)))
  private val timeIntegerRaw = List(Row(1, 20191010))
  private val timeIntegerRaw2 = List(Row(1, 20191020))
  private val timeIntegerDf =
    spark.createDataFrame(spark.sparkContext.parallelize(timeIntegerRaw), timeIntegerSchema)
  private val timeIntegerDf2 =
    spark.createDataFrame(spark.sparkContext.parallelize(timeIntegerRaw2), timeIntegerSchema)

  private val numericalSchema = StructType(List(StructField("integer", IntegerType)))
  private val numericalSchemaRaw = List(Row(1))
  private val numericalDf = spark.createDataFrame(spark.sparkContext.parallelize(numericalSchemaRaw), numericalSchema)

  private val incomingSchema =
    StructType(List(StructField("id", LongType),
                    StructField("field1", StringType),
                    StructField("field2", IntegerType),
                    StructField("field3", IntegerType)))
  private val incomingRaw = List(Row(1L,"A",null,4), Row(2L,null,6,null))
  private val incomingDf = spark.createDataFrame(spark.sparkContext.parallelize(incomingRaw), incomingSchema)

  private val storedSchema =
    StructType(List(StructField("id", LongType),
      StructField("field1", StringType),
      StructField("field2", IntegerType),
      StructField("field3", IntegerType)))
  private val storedRaw = List(Row(1L,"B",7,null), Row(2L,null,null,5))
  private val storedDf = spark.createDataFrame(spark.sparkContext.parallelize(storedRaw), storedSchema)

  private val longSchema = StructType(List(StructField("id", LongType)))
  private val longRaw = List(Row(1L), Row(2L))
  private val longDf = spark.createDataFrame(spark.sparkContext.parallelize(longRaw), longSchema)

  private val intSchema = StructType(List(StructField("id", IntegerType)))
  private val intRaw = List(Row(1), Row(2))
  private val intDf = spark.createDataFrame(spark.sparkContext.parallelize(intRaw), intSchema)

  private val emptySchema = StructType(List(StructField("id", IntegerType)))
  private val emptyRaw = List(Row())
  private val emptyDf = spark.createDataFrame(spark.sparkContext.parallelize(emptyRaw), emptySchema)

  private val minusSchema =
    StructType(List(StructField("column1", IntegerType), StructField("column2", StringType)))
  private val minus1Raw = List(Row(1, "a"), Row(2, "b"))
  private val minus2Raw = List(Row(1, "a"))
  private val minus1Df = spark.createDataFrame(spark.sparkContext.parallelize(minus1Raw), minusSchema)
  private val minus2Df = spark.createDataFrame(spark.sparkContext.parallelize(minus2Raw), minusSchema)
}

object CreateDataframe extends App {

  val dfBuilder = CreateDataframe()
  def getCardsDF = CreateDataframe().cardDF
  def getNumberDF = (CreateDataframe().df1, CreateDataframe().df2)
  def getTimeUnixDf: DataFrame = CreateDataframe().timeUnixDf
  def getTimeUnixSecondsDf: DataFrame = CreateDataframe().timeUnixSecondsDf
  def getNumericalDf: DataFrame = CreateDataframe().numericalDf
  def getIncomingDf: DataFrame = CreateDataframe().incomingDf
  def getStoredDf: DataFrame = CreateDataframe().storedDf
  def getTimeIntegerFormatDf: DataFrame = CreateDataframe().timeIntegerDf
  def getTimeIntegerFormatDf2: DataFrame = CreateDataframe().timeIntegerDf2
  def getLongDf: DataFrame = CreateDataframe().longDf
  def getIntDf: DataFrame = CreateDataframe().intDf
  def getEmptyDf: DataFrame = CreateDataframe().emptyDf
  def getMinus1Df: DataFrame = CreateDataframe().minus1Df
  def getMinus2Df: DataFrame = CreateDataframe().minus2Df
}
