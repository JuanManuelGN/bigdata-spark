package features

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case class DataframeFunctions() {

  def minus(df1: DataFrame, df2: DataFrame): DataFrame = df1.except(df2)
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
