package features

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

trait Group {

  /**
    * Dado un dataframe con 3 campos (id, descripción, fecha) elegir el id y la descrioción. Si
    * hubiera mas de un registro para un mismo id coger el id y la descripción del que tenga la
    * fecha mas alta
    */
  def groupByMaxDate(df: DataFrame): DataFrame = {
    df.groupBy(col("col1"))
      .agg(max(col("col3")).as("col3"))
      .join(df, Seq("col1","col3")).select("col1", "col2")
  }

}

object GroupByMaxDate extends App with Group {
  val df = CreateDataframe.getGroupDf

  val response = groupByMaxDate(df)

  df.show
  response.show
}
