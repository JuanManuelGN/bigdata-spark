package features

import config.SparkConfig
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

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
    val schema = StructType(List(StructField("column1", IntegerType),
      StructField("column2", StringType),
      StructField("date", IntegerType)))
    spark.createDataFrame(spark.sparkContext.parallelize(rowsOutput), schema)
  }

  /**
    * Devuelve si un dato se encuentra en una columna de un dataframe
    * @param df DataFrame
    * @param x dato a buscar
    * @return
    */
  def contains(df: DataFrame, x: Int): DataFrame = df.select(col(df.columns.head).contains(x))

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

  def isValid(col: Column): Column = col.isNotNull && !col.equals("")

  /**
    * Cuenta la cantidad de unos que hay en las columnas de un dataframe, como resultado da otra
    * dataframe con la misma cantidad de columnas con una única fila que contiene el número de unos
    * de cada columna
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
  def count: DataFrame => DataFrame = df => {

    val columnList = df.columns
    val projection = columnList.map(c => sum(when(col(c) === 1, 1)).as(c))

    df.select(projection: _*)
  }

  def showDfs(dfs: List[DataFrame]): Unit = dfs.foreach(df => df.show)
}

trait DfRunner extends App with DataframeFunctions

object Minus extends DfRunner {
  val df1 = CreateDataframe.getMinus1Df
  val df2 = CreateDataframe.getMinus2Df

  val idsDf1 = df1.select("column1").distinct.rdd.map(row => row.getInt(0)).collect()
  val idsDf2 = df2.select("column1").distinct.rdd.map(row => row.getInt(0)).collect()
  val sharedIds = idsDf1.toSet.intersect(idsDf2.toSet).toList

  val df1Crossdf2 =
    df1.filter(col("column1").isin(sharedIds: _*))
  val df2Crossdf1 =
    df2.filter(col("column1").isin(sharedIds: _*))

  val response = minus(df1, df2)

  showDfs(List(df1, df2, df1Crossdf2, df2Crossdf1, response))
}
object Except extends DfRunner {
  val df = CreateDataframe.getIdDf
  val emptyDf = df.filter(col("id") === 0)
  val exceptDf = minus(df, emptyDf)

  df.show
  emptyDf.show
  exceptDf.show

  df.union(emptyDf).show

  val listin: List[Int] = List()

  df.filter(col("id").isin(listin: _*)).show
}
object DeleteDuplicate extends DfRunner with SparkConfig {
  val duplicateDf = CreateDataframe.getDuplicateRowDf

  val cleaned = deleteDuplicates(duplicateDf)

  showDfs(List(duplicateDf, cleaned))
}
object Contains extends DfRunner {
  val df = CreateDataframe.getIntDf
  val x = 1

  val response = contains(df, x)

  showDfs(List(df, response))
}
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
object FilterNotEqual extends DfRunner {
  val df: DataFrame = CreateDataframe.getFilterNotEqualDf
  val response = filterNotEqual(df)

  showDfs(List(df, response))
}
object IsValid extends DfRunner {
  val df = CreateDataframe.getIntDf
  val response = isValid(df.col("id"))

  val result = df.withColumn("isValid", response)

  showDfs(List(df, result))
}

object Count extends DfRunner {
  val df = CreateDataframe.getCountDf
  val response = count(df)

  showDfs(List(df, response))
}
