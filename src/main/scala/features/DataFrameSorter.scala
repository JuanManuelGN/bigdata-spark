package features

import config.SparkConfig

/**
  * En esta clase se pueden ver varios métodos de ordenación de un DataFrame, tanto de sus filas
  * como de sus columnas
  */
object DataFrameSorter extends App with SparkConfig {

  /**
    * Reordenar las columnas de un dataframe
    */
  val df = CreateDataframe.getCardsDF

  df.printSchema

  val reorderedDf = df.select("load_date", "subtipo_tarj", "tipo_tarj")

  reorderedDf.printSchema
}
