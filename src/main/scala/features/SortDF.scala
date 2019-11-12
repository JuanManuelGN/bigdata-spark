package features

import config.SparkConfig

object SortDF extends App with SparkConfig {

  /**
    * Reordenar las columnas de un dataframe
    */
  val df = CreateDataframe.getCardsDF
  df.printSchema()

  val reorderedDf = df.select("load_date", "subtipo_tarj", "tipo_tarj")
  reorderedDf.printSchema()
}
