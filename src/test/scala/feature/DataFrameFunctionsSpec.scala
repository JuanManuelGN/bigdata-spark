package feature

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import features.{CreateDataframe, DataframeFunctions}
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec
import spark.SharedSparkSession
import utils.TestingUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

class DataFrameFunctionsSpec
  extends FlatSpec
    with SharedSparkSession
    with DataFrameSuiteBase
    with DataframeFunctions {

  /**
    * Map each table with the expected data
    */
  lazy val expectedResults: Map[String, DataFrame] = TestingUtils.loadExpectedLookupDfs

  it should "Contar la cantidad de unos que hay en cada columna" in {
    val df = CreateDataframe.getCountDf
    val response = count(df)

    val projection = response.columns.map(c => col(c).cast(IntegerType))

    val responseToTest = response.select(projection: _*)

    df.printSchema()
    response.printSchema()
    expectedResults("COUNT").printSchema()
    responseToTest.printSchema()

    df.show
    response.show
    expectedResults("COUNT").show
    responseToTest.show

    assertDataFrameEquals(responseToTest, expectedResults("COUNT"))
  }
}
