package spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.scalatest.Suite

/**
  * SparkSession for testing
  */
trait SharedSparkSession extends DataFrameSuiteBase {
  self: Suite =>

  /**
    * Make the spark session implicit
    */
  implicit lazy val sparkSession: SparkSession = spark

  /**
    * Compares if two [[DataFrame]]s are equal, checks the schema and then if that
    * matches checks if the rows are equal.
    */
  override def assertDataFrameEquals(expected: DataFrame, result: DataFrame) {
    super.assertDataFrameEquals(
      normalizeNullable(expected, true),
      normalizeNullable(result, true)
    )
  }

  /**
    * Return the same dataFrame with all fields with the desired nullable flag
    * @param dataFrame dataFrame
    * @param nullable nullable flag
    * @return dataFrame
    */
  def normalizeNullable(dataFrame: DataFrame, nullable: Boolean): DataFrame = {
    val newSchema = StructType(dataFrame.schema.map(_.copy(nullable = nullable)))
    spark.createDataFrame(dataFrame.rdd, newSchema)
  }

  /**
    * Cast all columns of a inputDf to have the same type as in the
    * desiredDf.
    * @param inputDf df to be casted
    * @param desiredSchema desired schema
    * @return casted dataFrame
    */
  def castTo(inputDf: DataFrame, desiredSchema: StructType): DataFrame = {
    val desiredSchemaMap = desiredSchema.map(s => (s.name.toLowerCase, s)).toMap
    val castingProjection: Seq[Column] = inputDf.schema.map(field =>
      desiredSchemaMap
        .get(field.name.toLowerCase)
        .filterNot(_.dataType.equals(field.dataType))
        .map(desiredField => col(field.name).cast(desiredField.dataType).alias(field.name))
        .getOrElse(col(field.name))
    )
    inputDf.select(castingProjection: _*)
  }

}

