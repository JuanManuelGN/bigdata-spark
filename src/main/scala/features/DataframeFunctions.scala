package features

import config.SparkConfig
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

case class DataframeFunctions() extends SparkConfig {

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
  def deleteDuplicates(df: DataFrame): DataFrame = {
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

  def contains(df: DataFrame, x: Int): DataFrame = df.select(col(df.columns.head).contains(x))

  def fillNull(df: DataFrame): DataFrame = df.na.fill("XX").na.fill(0)
}

object DataframeFunctions extends App {
  val df1 = CreateDataframe.getMinus1Df
  val df2 = CreateDataframe.getMinus2Df

  val idsDf1 = df1.select("column1").distinct.rdd.map(row => row.getInt(0)).collect()
  val idsDf2 = df2.select("column1").distinct.rdd.map(row => row.getInt(0)).collect()
  val sharedIds = idsDf1.toSet.intersect(idsDf2.toSet).toList

  val df1Crossdf2 =
    df1.filter(col("column1").isin(sharedIds: _*))
  val df2Crossdf1 =
    df2.filter(col("column1").isin(sharedIds: _*))

  val functions = new DataframeFunctions()

  val minus = functions.minus(df1, df2)

  minus.show
}
object Except extends App {
  val df = CreateDataframe.getIdDf
  val emptyDf = df.filter(col("id") === 0)
  val exceptDf = DataframeFunctions().minus(df, emptyDf)

  df.show
  emptyDf.show
  exceptDf.show

  df.union(emptyDf).show

  val listin: List[Int] = List()

  df.filter(col("id").isin(listin: _*)).show
}
object CleanDuplicate extends App {
  val duplicateDf = CreateDataframe.getDuplicateRowDf

  duplicateDf.show

  val cleaned = DataframeFunctions().deleteDuplicates(duplicateDf)

  cleaned.show
}
object Contains extends App {
  val df = CreateDataframe.getIntDf
  val x = 1

  DataframeFunctions().contains(df, x).show
}
object FillNull extends App {
  val df: DataFrame = CreateDataframe.getDfWithNullValues
  val x:DataFrame = DataframeFunctions().fillNull(df)
  x.show
}
