package features

import config.SparkConfig

import org.apache.spark.sql.functions._

import org.apache.spark.sql.DataFrame

/**
  * En esta clase se va a calcular una mezcla entre dataframes siguiendo el siguiente criterio:
  * Imaginemos que tenemos que unir dos dataframes con la misma estructura, uno de ellos lo tenemos
  * guardado en el sistema de persistencia (stored) y el otro es un dataframe que hemos calculado
  * en la ejecución actual. Puesto que es una actualización se deben conservar los datos guardados
  * ya que los que no han sido modificados en el df calculado en la ejecución está a nul. Un ejemplo
  * a continuación
  * storedDf =   [1L,"B",7,   null]
  * incomingDf = [1L,"A",null,4]
  * resultado =  [1L,"A",7,   5]
  */
class Merge extends SparkConfig {

  def merge (incomingDf: DataFrame, storedDf: DataFrame): DataFrame = {

    import spark.implicits._

    val incomingColumns = incomingDf.columns.toList
    val storedColumns = storedDf.columns.toList


    val renamedIncomingColumns = incomingColumns.map(name => name.concat("_"))
    val renamedStoredColumns = storedColumns.map(name => name.concat("__"))

    val incomingRenamedDf = incomingDf.toDF(renamedIncomingColumns: _*)
    val storedRenamedDf = storedDf.toDF(renamedStoredColumns: _*)

    val joinedDf = incomingRenamedDf
      .join(storedRenamedDf, incomingRenamedDf("id_") === storedRenamedDf("id__"))

    incomingColumns.foldLeft(joinedDf)((df, column) =>
      df.withColumn(column, when(col(column + "_").isNotNull, col(column + "_")).otherwise(col(column + "__"))))
      .select(incomingColumns.head, incomingColumns.tail: _*)
  }

}
object Merge extends App {
  val incomingDf = CreateDataframe.getIncomingDf
  val storedDf = CreateDataframe.getStoredDf

  val merged = new Merge().merge(incomingDf, storedDf)
  storedDf.show
  incomingDf.show
  merged.show
}
