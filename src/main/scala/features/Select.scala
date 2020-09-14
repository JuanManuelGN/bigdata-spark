package features

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.functions._

trait Select {

  def redundantSelect(df: DataFrame): DataFrame = {
    val projection = Seq(
      col("id").as("ID").cast(LongType),
      lit(-9999).as("NReturnReason"))
    df
      .select("id")
      .distinct
      .select(projection: _*)
  }

  def notRedundantSelect(df: DataFrame): DataFrame = {
    val projection = Seq(
      col("id").as("ID").cast(LongType),
      lit(-9999).as("NReturnReason"))

    df.select(projection: _*).distinct()
  }

  def addColumnProjection(df: DataFrame): Seq[Column] = {
    Seq(
      when(col("count") > 20, lit(2)).otherwise(lit(1)).alias("Over20"),
      lit(2).alias("tete")
    )
  }
}

object RedundantSelect extends App with Select {
  val df = CreateDataframe.getRedundantSelect.cache()
  df.unpersist()
  val response = redundantSelect(df)
  val responseNotRedundant = notRedundantSelect(df)

  Df.showAnPrintSchema(List(df, response, responseNotRedundant))
}

object AddColumnProjection extends App with Select {
  val lookup = CreateDataframe.getAddColumnProjectionL
  val df = CreateDataframe.getAddColumnProjection
  val response = df.select(addColumnProjection(lookup): _*)

  Df.showAnPrintSchema(List(lookup, df, response))
}
