package features

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.FlatSpec
import spark.SharedSparkSession

class GroupByMaxSpec
  extends FlatSpec
    with SharedSparkSession
    with DataFrameSuiteBase
    with Group {

  val idColName = "id"
  val nColName = "n"
  val aggColName = "agg"

  lazy val schema =
    StructType(List(
      StructField(idColName, IntegerType),
      StructField(nColName, IntegerType))
    )
  lazy val raw = List(Row(1,1), Row(2,2), Row(1,2),Row(4,5))
  lazy val df = spark.createDataFrame(spark.sparkContext.parallelize(raw), schema)
  lazy val response =
    groupByMax(df, idColName, nColName, aggColName)
      .orderBy(col(idColName))

  it should "Agrupación de dataframe por columna id y cogiendo el max" in {
    val expectedSchema =
      StructType(List(
        StructField(idColName, IntegerType),
        StructField(aggColName, IntegerType))
      )
    val expectedDF =
      spark.createDataFrame(
        spark.sparkContext.parallelize(
          List(Row(2,2), Row(1,2),Row(4,5))),
        expectedSchema)
        .orderBy(col(idColName))

    assertDataFrameEquals(expectedDF, response)
  }

}
