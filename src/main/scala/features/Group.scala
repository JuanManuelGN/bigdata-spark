package features

import config.SparkConfig

import java.util
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Métodos sobre dataframes para explorar las posibilidades que
  * nos ofrece la agrupación
  */
trait Group {

  /**
    * Dado un dataframe con 2 campos (id, field2) elegir el id y el máximo de la columna field2.
    */
  def groupByMax(df: DataFrame,
                 groupingColName: String,
                 maxColName: String,
                 aggColName: String): DataFrame =
    df.groupBy(col(groupingColName))
      .agg(max(col(maxColName)).as(aggColName))

}
