package features

import config.SparkConfig
import org.apache.spark
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

trait DataframeFunctions {

  /**
    * Elimina las filas del primer dataframe que aparecen en el segundo dataframe
    * @param df1
    * @param df2
    * @return
    */
  def minus(df1: DataFrame, df2: DataFrame): DataFrame = df1.except(df2)
  /**
    * Dado un dataframe con filas con identificadores duplicados, este método agrupará todos esos
    * registros con un mismo id en uno solo. En el caso de que el resto de campos sean distintos
    * cogerá el último de los mismos, por ejemplo:
    * id  name
    * --------
    * 1   pepe
    * 2   juan
    * 1   alberto
    *
    * El resultado sería
    *
    * id  name
    * --------
    * 1   alberto
    * 2   juan
    * @param df dataframe
    * @return dataframe without duplicates rows
    */
  def deleteDuplicates(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val rows = df.collect().toList
    val rowsOutput = rows.foldLeft(List(Row()))((rows, row) => {
      val incomingId = row.getAs[Integer](0)
      if (rows.head.size > 0) {
        val duplicate = rows.filter(x => x.getAs[Integer](0) == incomingId)
        if (duplicate.isEmpty) {
          rows ++ List(row)
        } else {

          val storedColumn2 = rows.filter(x => x.getAs[Integer](0) == incomingId).head.getAs[String](1)
          val incomingColumn2 = row.getAs[String](1)

          val column1 = row.getAs[Integer](0)

          val column2 = if (incomingColumn2 == null || incomingColumn2.isEmpty) {
            storedColumn2
          } else {
            incomingColumn2
          }
          val date = row.getAs[Integer](2)
          rows.filter(x => x.getAs[Integer](0) != column1) ++ List(Row(column1, column2, date))
        }
      } else {
        List(row)
      }
    }
    )
    val schema = StructType(List(StructField("col1", IntegerType),
      StructField("col2", StringType),
      StructField("col3", IntegerType)))
    spark.createDataFrame(spark.sparkContext.parallelize(rowsOutput), schema)
  }

  /**
    * Devuelve si un dato se encuentra en una columna de un dataframe
    * @param df DataFrame
    * @param x dato a buscar
    * @return
    */
  def contains(df: DataFrame, x: Int, column: String): DataFrame = df.select(col(column).contains(x))

  /**
    * Rellena con "s los campos nulos de tipo string y con i los campos nulos de tipo entero
    * @param df dataframe con valores nulos
    * @param s cadena con el valor a sustituir en las columnas de tipo string
    * @param i número entero a sustituir en las columnas de tipo Int
    * @return dataframe sin valores nulos
    */
  def fillNull : (DataFrame, String, Int) => DataFrame = (df, s, i) => df.na.fill(s).na.fill(i)

  def fillNullSeveralCases: (DataFrame, Map[String, Column]) => DataFrame =
    (df, default) => {
      val p = df.columns.map(c =>
        if (default.contains(c)) {
          when(col(c).isNull, default(c)).otherwise(df(c)).as(c)
        } else {
          df(c).as(c)
        })
      df.select(p: _*)
    }

  /**
    * La siguiente función .na.fill no se puede usar con el tipo Column
    * @return
    */
  def fillNullWithDefaultValues : (DataFrame, Map[String, Any]) => DataFrame = (df, default) =>
    df.na.fill(default) // no funciona con nulos

  def fillNull1: (DataFrame, Map[String, Column]) => DataFrame =
    (df, defaults) => {
      val projection = df.columns.map( column =>
        defaults.get(column)
          .map(defaultCol => coalesce(col(column), defaultCol).as(column)).getOrElse(col(column).as(column))
      )
      df.select(projection: _*)
    }

  def filterNotEqual(df: DataFrame): DataFrame = df.filter(col("col1") =!= 2)

  /**
    * Dada una columna verifica que no es nula ni vacía
    * @param col
    * @return Column[Boolean]
    */
  def colIsValid(col: Column): Column = col.isNotNull && !col.equals("")

  /**
    * Cuenta la cantidad de unos que hay en las columnas de un dataframe, como resultado da otra
    * dataframe con la misma cantidad de columnas con una única fila que contiene la cantidad de
    * elementos encontrados en cada columna
    *
    * @return
    *
    * +----+----+----+
    * |col1|col2|col3|
    * +----+----+----+
    * |   1|   1|   3|
    * |   3|   2|   1|
    * |   2|   5|   1|
    * |   4|   1|   6|
    * +----+----+----+
    *         da como resultado:
    * +----+----+----+
    * |col1|col2|col3|
    * +----+----+----+
    * |   1|   2|   2|
    * +----+----+----+
    */
  def countOcurrencesEachCol(df: DataFrame, n: Int): DataFrame = {

    val columnList = df.columns
    val projection = columnList.map(c => sum(when(col(c) === n, 1)).as(c))

    df.select(projection: _*)
  }

  def countEquals(df:DataFrame): DataFrame = {
    val OkValueInt = "r"
    val countEqualOkValue = (column: String) => count(when(col(column).equalTo(OkValueInt), true))
    val countIsNotNull = (column: String) => count(when(col(column).isNotNull, 1))

    df.groupBy("id")
      .agg(countEqualOkValue("col1").as("equal"),
        countIsNotNull("col2").as("notnull"),
        count("col2").alias("notnullshort")).orderBy("id")
  }

  def showDfs(dfs: List[DataFrame]): Unit = dfs.foreach(_.show)
  def showAnPrintSchema(dfs: List[DataFrame]): Unit = {
    dfs.foreach(_.printSchema)
    showDfs(dfs)
  }

  def flatUnion: List[List[DataFrame]] => DataFrame =
    listOfList => listOfList.flatten.reduce(_ union _)
}

trait DfRunner extends App with DataframeFunctions

object FillNull extends DfRunner {
  val df: DataFrame = CreateDataframe.getDfWithNullValues
  val s: String = "XX"
  val i: Int = 0

  val response: DataFrame = fillNull(df, s, i)

  showDfs(List(df, response))
}
object FillNullWithDefaultValues extends DfRunner {
  val df: DataFrame = CreateDataframe.getDfWithNullValues
  val default: Map[String, Any] = Map("col1" -> 20, "col2" -> "patata")

  val response: DataFrame = fillNullWithDefaultValues(df, default)

  showDfs(List(df, response))
}
object FillNullSeveralCases extends DfRunner {
  val df: DataFrame = CreateDataframe.getDfWithNullValues

  /**
    * Dos formas de montar el mapa con las columnas y los valores por defecto
    */
  // forma 1
  val columns = df.columns.toList ++ List("pepe")
  columns.map(println(_))

  val default = columns.map {
    case x if x.equals("col3") => "col3" -> lit(6)
    case "col1" => "col1" -> lit(5)
    case x => x -> lit("patata")
  }.toMap
  default.map(println(_))

  // forma 2
//  val default: Map[String, Column] = Map("col1" -> lit(5), "col2" -> lit("patata"))

  val response: DataFrame = fillNullSeveralCases(df, default)

  showDfs(List(df, response))
}
object FillNull1 extends DfRunner {
  val df: DataFrame =
    CreateDataframe.getDfWithNullValues
      .withColumn("a", lit(5))
      .withColumn("b", col("a") * 10)
  val default: Map[String, Column] = Map("col1" -> lit(20)/*, "col2" -> lit("patata")*/)

  val response: DataFrame = fillNull1(df, default)

  showDfs(List(df, response))
  df.explain(true)
}

object FlatUnion extends DfRunner {
  val df: DataFrame =
    CreateDataframe.getDfWithNullValues
      .withColumn("a", lit(5))
      .withColumn("b", col("a") * 10)

  val response: DataFrame = flatUnion(List(List(df, df), List(df, df)))

  showDfs(List(df, response))
  df.explain(true)
}

/**
  * Prueba el método coalesce: Esta función coge una lista de columas y devuelve la que no sea nula
  * dando preferencia de izquierda a derecha:
  * Con los datos (1,null,null) devuelve 1
  * Con los datos (null,1,null) devuelve 1
  * Con los datos (null,null,null) devuelve null
  * Con los datos (3,1,null) devuelve 3
  */
object Coalesce extends DfRunner with SparkConfig {


  val nullSchemaId =
    StructType(List(
      StructField("id", IntegerType),
      StructField("col1", StringType),
      StructField("col2", StringType)))

  val incomingRaw = List(
    Row(1,"r", "A"),
    Row(2,null, "b"),
    Row(3,null, null))

  val storedRaw = List(
    Row(1,"r", "a"),
    Row(2,"t", "b"))

  val incoming = spark.createDataFrame(spark.sparkContext.parallelize(incomingRaw), nullSchemaId)
  val stored = spark.createDataFrame(spark.sparkContext.parallelize(storedRaw), nullSchemaId)

  val renamedProjection = incoming.columns.map(c => col(c).alias(c.concat("_")))
  val incomingRenamed = incoming.select(renamedProjection: _*)


  val joinedDf = stored.join(incomingRenamed, col("id") === col("id_"), "full")

  val updatedEntriesProjection = stored.columns.map(column =>
    coalesce(col(column.concat("_")), col(column)).alias(column)
  )
  val updatedDf = joinedDf.select(updatedEntriesProjection: _*)

  val response = fillNull1(updatedDf, Map("col1" -> lit("5"), "col2" -> lit("hola")))


  incoming.show
  stored.show
  incomingRenamed.show
  joinedDf.show
  updatedDf.show
  response.show

}

/**
  * Prueba el método coalesce: Esta función coge una lista de columas y devuelve la que no sea nula
  * dando preferencia de izquierda a derecha:
  * Con los datos (1,null,null) devuelve 1
  * Con los datos (null,1,null) devuelve 1
  * Con los datos (null,null,null) devuelve null
  * Con los datos (3,1,null) devuelve 3
  */
object CountOk extends DfRunner with SparkConfig {

  val schema = StructType(List(
    StructField("id", IntegerType),
    StructField("col1", StringType),
    StructField("col2", StringType)))

  val dfRaw = List(
    Row(1,"r", "A"),
    Row(1,"t", "A"),
    Row(2,null, "b"),
    Row(3,null, null))

  val df = spark.createDataFrame(spark.sparkContext.parallelize(dfRaw), schema)

  val response = countEquals(df)

  showAnPrintSchema(List(df, response))

}
