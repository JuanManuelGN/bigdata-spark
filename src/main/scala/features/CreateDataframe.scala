package features

import config.SparkConfig
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

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
  private val intRaw = List(Row(1), Row(null), Row(12))
  private val intDf = spark.createDataFrame(spark.sparkContext.parallelize(intRaw), intSchema)

  private val emptySchema = StructType(List(StructField("id", IntegerType)))
  private val emptyRaw = List(Row())
  private val emptyDf = spark.createDataFrame(spark.sparkContext.parallelize(emptyRaw), emptySchema)

  private val idRaw = List(Row(1))
  private val idDf = spark.createDataFrame(spark.sparkContext.parallelize(idRaw), emptySchema)

  private val exceptSchema = StructType(List(StructField("col1", IntegerType)))
  private val exceptRaw = List(Row(3), Row(4))
  private val exceptDf = spark.createDataFrame(spark.sparkContext.parallelize(exceptRaw), exceptSchema)

  private val minusSchema =
    StructType(List(StructField("col1", IntegerType), StructField("col2", StringType)))
  private val minus1Raw = List(Row(1, "a"), Row(2, "b"))
  private val minus2Raw = List(Row(1, "a"))
  private val minus1Df = spark.createDataFrame(spark.sparkContext.parallelize(minus1Raw), minusSchema)
  private val minus2Df = spark.createDataFrame(spark.sparkContext.parallelize(minus2Raw), minusSchema)

  private val duplicateSchema =
    StructType(List(StructField("column1", IntegerType),
      StructField("column2", StringType),
      StructField("date", IntegerType)))
  private val duplicateRaw = List(Row(6, "a", 1), Row(2, null, null), Row(2, "6", 2), Row(2, null, 3), Row(1, "r", 3), Row(5, "T", 3))
//  private val duplicateRaw = List(Row(1, "a", 1), Row(2, "b", 1), Row(5, "T", 3))
  private val duplicateDf = spark.createDataFrame(spark.sparkContext.parallelize(duplicateRaw), duplicateSchema)

  private def nullSchema = StructType(List(StructField("col1", DecimalType(10, 3)),
                                           StructField("col2", StringType)))

  private val nullRaw = List(Row(BigDecimal(1), "a"), Row(null, "b"), Row(BigDecimal(3), null))
  private val nullValuesDf = spark.createDataFrame(spark.sparkContext.parallelize(nullRaw), nullSchema)

  private def decimalSchema = StructType(List(StructField("col1", StringType)))
  private val decimalRaw = List(Row("1500.000"))
  private val decimalDf = spark.createDataFrame(spark.sparkContext.parallelize(decimalRaw), decimalSchema)

  private def filterNotEqualSchema = StructType(List(StructField("col1", IntegerType)))
  private val filterNotEqualRaw = List(Row(1), Row(3), Row(2), Row(4))
  private val filterNotEqualDf = spark.createDataFrame(spark.sparkContext.parallelize(filterNotEqualRaw), filterNotEqualSchema)

  private def joinAndSumSchema = StructType(List(StructField("col1", IntegerType), StructField("col2", IntegerType)))
  private val joinAndSumRaw1 = List(Row(1,1), Row(3,2), Row(2,5), Row(4,0))
  private val joinAndSum1Df = spark.createDataFrame(spark.sparkContext.parallelize(joinAndSumRaw1), joinAndSumSchema)

  private val joinAndSumRaw2 = List(Row(1,6), Row(3,8), Row(2,7), Row(4,2))
  private val joinAndSum2Df = spark.createDataFrame(spark.sparkContext.parallelize(joinAndSumRaw2), joinAndSumSchema)

  private def decimalTypeSchema = StructType(List(StructField("col1", DecimalType(10, 3))))
  private val decimalTypeRaw = List(Row(BigDecimal(0)))
  private val decimalTypeDf = spark.createDataFrame(spark.sparkContext.parallelize(decimalTypeRaw), decimalTypeSchema)

  private def countSchema = StructType(List(StructField("col1", IntegerType),
    StructField("col2", IntegerType), StructField("col3", IntegerType)))
  private val countRaw = List(Row(1,1,3), Row(3,2,1), Row(2,5,1), Row(4,1,6))
  private val countDf = spark.createDataFrame(spark.sparkContext.parallelize(countRaw), countSchema)

  private val validSchema = StructType(List(StructField("col1", IntegerType)))
  private val validRaw = List(Row(1), Row(null), Row(12))
  private val validDf = spark.createDataFrame(spark.sparkContext.parallelize(validRaw), validSchema)

  private val groupchema =
    StructType(List(
      StructField("id", IntegerType),
      StructField("description", StringType),
      StructField("date", IntegerType)))
  private val groupRaw = List(Row(1,"hola",1), Row(2,"adios",2), Row(1,"hello",2),Row(4,"hello",5))
  private val groupDf = spark.createDataFrame(spark.sparkContext.parallelize(groupRaw), groupchema)

  private val joinAndSustituteValueSchema =
    StructType(List(
      StructField("col1", IntegerType),
      StructField("col2", StringType),
      StructField("col3", IntegerType)))
  private val joinAndSustituteValueRaw = List(Row(1,"OK",6), Row(2,"KO",2), Row(3,"PENDING",5))
  private val joinAndSustituteValueDf =
    spark.createDataFrame(spark.sparkContext.parallelize(joinAndSustituteValueRaw), joinAndSustituteValueSchema)

  private val joinAndSustituteValueSchema2 =
    StructType(List(
      StructField("col11", IntegerType),
      StructField("col22", StringType),
      StructField("col33", IntegerType)))
  private val joinAndSustituteValueRaw2 = List(Row(1,"OK",1), Row(2,"KO",2), Row(3,"PTE",2))
  private val joinAndSustituteValueDf2 =
    spark.createDataFrame(spark.sparkContext.parallelize(joinAndSustituteValueRaw2), joinAndSustituteValueSchema2)

  private val nullSchemaId =
    StructType(List(
      StructField("id", IntegerType),
      StructField("col1", DecimalType(10, 3)),
      StructField("col2", StringType)))

  private val nullRawId = List(Row(1,BigDecimal(1), "a"), Row(2,null, "b"), Row(3,BigDecimal(3), null))
  private val nullValuesIdDf = spark.createDataFrame(spark.sparkContext.parallelize(nullRawId), nullSchemaId)

  private val simpleJoinSchema =
    StructType(List(
      StructField("id", IntegerType),
      StructField("col1", DecimalType(10, 3)),
      StructField("col2", StringType)))

  private val simpleJoinRaw = List(Row(1,BigDecimal(1), "a"), Row(2,null, "b"), Row(3,BigDecimal(3), null))
  private val simpleJoinDf = spark.createDataFrame(spark.sparkContext.parallelize(nullRawId), nullSchemaId)

  private val whenIncomingSchema =
    StructType(List(
      StructField("idIncoming", IntegerType),
      StructField("result", StringType),
      StructField("type", StringType)))

  private val whenIncomingJoinRaw =
    List(Row(1, "KO", "manual"), Row(2,"OK", "manual"), Row(3, "PTE", null), Row(3, "OK", "manual"),
      Row(4, "OK", "automatic"), Row(5, "PTE", null), Row(5, "KO", "automatic"), Row(6, "OK", null),
      Row(8, "PTE", null))
  private val whenIncomingJoinDf = spark.createDataFrame(spark.sparkContext.parallelize(whenIncomingJoinRaw), whenIncomingSchema)

  private val whenStoredSchema =
    StructType(List(StructField("idStored", IntegerType),StructField("nAppReason", StringType)))

  private val whenStoredJoinRaw = List(Row(1, "PTE"), Row(2, null), Row(4, "PTE"), Row(5, null), Row(7, "PTE"))
  private val whenStoredJoinDf = spark.createDataFrame(spark.sparkContext.parallelize(whenStoredJoinRaw), whenStoredSchema)

  private val joinReduceSchema = StructType(List(
    StructField("id", IntegerType),
    StructField("col1", StringType)))
  private val joinReduceRaw1 = List(Row(1, "PTE"), Row(2, null), Row(4, "PTE"), Row(5, null), Row(7, "PTE"))
  private val joinReduceDf1 = spark.createDataFrame(spark.sparkContext.parallelize(joinReduceRaw1), joinReduceSchema)
  private val joinReduceRaw2 = List(Row(2, "KKK"), Row(4, "PTE"), Row(5, null), Row(10, "PTE"))
  private val joinReduceDf2 = spark.createDataFrame(spark.sparkContext.parallelize(joinReduceRaw2), joinReduceSchema)
  private val joinReduceDfList = List(joinReduceDf1, joinReduceDf2)

  private val redundantSelectSchema = StructType(List(
    StructField("id", IntegerType),
    StructField("col1", StringType)))
  private val redundantSelectRaw =
    List(Row(1, "PTE"), Row(1, null), Row(4, "PTE"), Row(5, null), Row(5, "PTE"))
  private val redundantSelect =
    spark.createDataFrame(spark.sparkContext.parallelize(redundantSelectRaw), redundantSelectSchema)

  private val addSumColDfSchema = StructType(List(
    StructField("id", IntegerType),
    StructField("col1", IntegerType),
    StructField("col2", IntegerType)
  ))
  private val addSumColDfRaw =
    List(Row(1, 1, 5), Row(2, 2, 5), Row(3, 1, 5), Row(4, 1, 7), Row(5, 3, 7))
  private val addSumColDf =
    spark.createDataFrame(spark.sparkContext.parallelize(addSumColDfRaw), addSumColDfSchema)

  private val renameAndCastDfSchema = StructType(List(
    StructField("id", IntegerType),
    StructField("col1", IntegerType),
    StructField("col2", IntegerType),
    StructField("col3", IntegerType)
  ))
  private val renameAndCastDfRaw =
    List(Row(1, 1, 5, 8), Row(2, 2, 5, 9), Row(3, 1, 5, 3), Row(4, 1, 7, 5), Row(5, 3, 7, 6))
  private val renameAndCastDf =
    spark.createDataFrame(spark.sparkContext.parallelize(renameAndCastDfRaw), renameAndCastDfSchema)
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
  def getDuplicateRowDf: DataFrame = CreateDataframe().duplicateDf
  def getIdDf: DataFrame = CreateDataframe().idDf
  def getDfWithNullValues: DataFrame = CreateDataframe().nullValuesDf
  def getDecimalDf: DataFrame = CreateDataframe().decimalDf
  def getFilterNotEqualDf: DataFrame = CreateDataframe().filterNotEqualDf
  def getJoinAndSum1: DataFrame = CreateDataframe().joinAndSum1Df
  def getJoinAndSum2: DataFrame = CreateDataframe().joinAndSum2Df
  def getDecimalTypeDf = CreateDataframe().decimalTypeDf
  def getCountDf = CreateDataframe().countDf
  def getValidDf = CreateDataframe().validDf
  def getExceptDf = CreateDataframe().exceptDf
  def getGroupDf = CreateDataframe().groupDf
  def getJoinAndSustituteValueDf =
    (CreateDataframe().joinAndSustituteValueDf, CreateDataframe().joinAndSustituteValueDf2)
  def getnullValuesIdDf = CreateDataframe().nullValuesIdDf
  def getSimpleJoinDf = CreateDataframe().simpleJoinDf
  def getWhenDfs = (CreateDataframe().whenIncomingJoinDf, CreateDataframe().whenStoredJoinDf)
  def getJoinReduce: List[DataFrame] = CreateDataframe().joinReduceDfList
  def getRedundantSelect: DataFrame = CreateDataframe().redundantSelect
  def getAddSumColDf: DataFrame = CreateDataframe().addSumColDf
  def getRenameAndCast: DataFrame = CreateDataframe().renameAndCastDf

  val numberDf = getNumberDF._1

  numberDf.printSchema

//  val numberModifiedDf = numberDf.withColumn("c2", when(col("c1") === 1560, lit(2000)))
//
//  val schema = StructType(List(StructField("c1", IntegerType), StructField("c2", IntegerType)))
//  val rdd = numberModifiedDf.rdd
//  val newNumberDf = spark.createDataFrame(spark.sparkContext.parallelize(rdd.collect), schema)
//  newNumberDf.printSchema

  val numberNewSchema = alterSchema(numberDf)
  numberNewSchema.printSchema

  def alterSchema(df: DataFrame): DataFrame ={
    val schema = df.schema
    val newSchema =
      StructType(schema.map(field => StructField(field.name, field.dataType, true, field.metadata)))
    df.sqlContext.createDataFrame( df.rdd, newSchema )
  }


}
object DecimalTypeRun extends App {
  CreateDataframe.getDecimalTypeDf.show
}
