package features

import java.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

trait Group {

  /**
    * Dado un dataframe con 3 campos (id, descripción, fecha) elegir el id y la descrioción. Si
    * hubiera mas de un registro para un mismo id coger el id y la descripción del que tenga la
    * fecha mas alta
    */
  def groupByMaxDate(df: DataFrame): DataFrame = {
    df.groupBy(col("id"))
      .agg(max(col("date")).as("date"))
      .join(df, Seq("id","date")).select("id", "description")
  }

}

object GroupByMaxDate extends App with Group {
  val df = CreateDataframe.getGroupDf

  val response = groupByMaxDate(df)

  Df.showAnPrintSchema(List(df, response))
}


