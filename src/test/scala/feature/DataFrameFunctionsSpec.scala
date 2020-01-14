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
    val expected = expectedResults("COUNT")

    val response = count(df)

    val projection = response.columns.map(c => col(c).cast(IntegerType))

    val responseToTest = response.select(projection: _*)

//    showAnPrintSchema(List(df, response, countDf, responseToTest))

    assertDataFrameEquals(responseToTest, expected)
  }

  it should "Comprobar la validez de los datos de una columna" in {
    val df = CreateDataframe.getValidDf
    val expected = expectedResults("VALID")

    val response = colIsValid(df.col("col1"))
    val responseToTest = df.withColumn("isValid", response)

//    showAnPrintSchema(List(df, valid, responseToTest))

    assertDataFrameEquals(responseToTest, expected)
  }

  it should "Filtra las filas que no cumplan la condición" in {
    val df = CreateDataframe.getFilterNotEqualDf
    val expected = expectedResults("FILTER_NOT_EQUAL")

    val response = filterNotEqual(df)

    //    showAnPrintSchema(List(df, valid, responseToTest))

    assertDataFrameEquals(response, expected)
  }

  it should "DataFrame except DataFrame" in {
    val df = CreateDataframe.getExceptDf
    val response = minus(df, df)

    //    showAnPrintSchema(List(df, response))

    assert(response.isEmpty)
  }

  it should "DataFrame minus DataFrame" in {
    val df1 = CreateDataframe.getMinus1Df
    val df2 = CreateDataframe.getMinus2Df
    val expected = expectedResults("MINUS")

    val response = minus(df1, df2)

//    showDfs(List(df1, df2, df1Crossdf2, df2Crossdf1, response))

    assertDataFrameEquals(response, expected)
  }

  it should "Delete duplicates" in {
    val df = CreateDataframe.getDuplicateRowDf
    val expected = expectedResults("NODUPLICATES")

    val response = deleteDuplicates(df)

//    showDfs(List(df, expected, response))

    assertDataFrameEquals(response, expected)
  }
}
