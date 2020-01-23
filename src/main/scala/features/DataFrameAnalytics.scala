package features

import config.SparkConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

trait DataFrameAnalytics extends SparkConfig {

  def getIdList(df: DataFrame, colName: String = "id"): Array[Long] = {
    df.select(colName).distinct.rdd.map(row => row.getLong(0)).collect()
  }

  def viewPlan = {
    import spark.implicits._

    case class Persona(id: String, nombre: String, edad: Int)

    val peopleDataset  = Seq("Bob", "Joe").toDF("nombre")

    val query = peopleDataset.groupBy("nombre").count().as("total")

    query.explain(extended = true)
  }
}
object ViewPlan extends App with DataFrameAnalytics {viewPlan}

object DataFrameAnalytics extends App with DataFrameAnalytics {

  val df = CreateDataframe.getLongDf
  df.printSchema()
  val idList = getIdList(df).toList
  println(idList)

  val IntegerDf = CreateDataframe.getIntDf
  IntegerDf.printSchema()
  val idIntList = getIdList(df).toList
  println(idIntList)

//  val emptyDf = CreateDataframe.getEmptyDf
//  emptyDf.printSchema()
//  val idEmptyList = DataFrameAnalytics().getIdList(emptyDf).toList
//  println(idEmptyList)

  val numericDf = CreateDataframe.getNumberDF._1
  val numericRenamedDf = numericDf.select(col("c1").as("id").cast(LongType))
  numericDf.printSchema()
  numericRenamedDf.printSchema()
  val idNoTypeList = getIdList(numericRenamedDf).toList
  println(idNoTypeList)

}
