package features

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

case class DataFrameAnalytics() {

  def getIdList(df: DataFrame, colName: String = "id"): Array[Long] = {
    df.select(colName).distinct.rdd.map(row => row.getLong(0)).collect()
  }
}
object DataFrameAnalytics extends App {

  val df = CreateDataframe.getLongDf
  df.printSchema()
  val idList = DataFrameAnalytics().getIdList(df).toList
  println(idList)

  val IntegerDf = CreateDataframe.getIntDf
  IntegerDf.printSchema()
  val idIntList = DataFrameAnalytics().getIdList(df).toList
  println(idIntList)

//  val emptyDf = CreateDataframe.getEmptyDf
//  emptyDf.printSchema()
//  val idEmptyList = DataFrameAnalytics().getIdList(emptyDf).toList
//  println(idEmptyList)

  val numericDf = CreateDataframe.getNumberDF._1
  val numericRenamedDf = numericDf.select(col("c1").as("id").cast(LongType))
  numericDf.printSchema()
  numericRenamedDf.printSchema()
  val idNoTypeList = DataFrameAnalytics().getIdList(numericRenamedDf).toList
  println(idNoTypeList)

}
