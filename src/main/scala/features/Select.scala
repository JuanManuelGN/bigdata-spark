package features

import org.apache.spark.sql.DataFrame
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
}

object RedundantSelect extends App with Select {
  val df = CreateDataframe.getRedundantSelect
  val response = redundantSelect(df)
  val responseNotRedundant = notRedundantSelect(df)

  Df.showAnPrintSchema(List(df, response, responseNotRedundant))

}
