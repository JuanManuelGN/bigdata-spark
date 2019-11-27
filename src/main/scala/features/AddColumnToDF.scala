package features

import config.SparkConfig
import features.Rows.spark
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._


case class AddColumnToDF() {

  def addColumnWithUdfFunction(df: DataFrame): DataFrame = {
    def codPaisColumn: (String, String) => String =
      (t, st) =>
        if(t.equals("506")) {
          st match {
            case "013" | "016" => "DE"
            case "014" | "017" => "NL"
            case "015" | "018" => "PT"
            case _ => "ES"
          }
        }
        else {
          "ES"
        }

    val myUdf = udf(codPaisColumn)

    df.withColumn("new_column",
      myUdf(col("tipo_tarj"), col("subtipo_tarj")))
  }
  /**
    * Se añade una columna con una fecha dada en formato xxx/yyy/.../intake_date=yyyyMMdd
    * @param df
    * @param date
    */
  def addDataToDfFromString(df: DataFrame, input: String): DataFrame = {
    val partition = input.substring(input.lastIndexOf("/"), input.length).split("=").apply(1)
    val sDate = partition.toString
    val ssDate=  sDate.substring(0, 4)
        .concat("-")
        .concat(sDate.substring(4, 6))
        .concat("-")
        .concat(sDate.substring(6, 8))

    val partialDf = df.withColumn("c2", lit(ssDate))
    partialDf.show
    partialDf.printSchema()
    partialDf.select(to_date(col("c2"), "yyyy-MM-dd").as("c3"))
  }
}
object AddColumnToDFWithUdfFunction extends App {

  import spark.implicits._

  val cardsDF = CreateDataframe.getCardsDF

  val addedUdfColumn = AddColumnToDF().addColumnWithUdfFunction(cardsDF)

  addedUdfColumn.show

  /*
  /**
    * Añadir una nueva columna que sea la concatenación de otras dos. Se realizará un casting a
    * entero en el momento de la concatención para comprobar si al hacerlo elimina los ceros a la
    * izquierda de los strings 00056 por ejemplo.
    */
  val dfWithNewColumn =
    cardsDF.withColumn(
      "concat",
      concat(col("tipo_tarj"), col("subtipo_tarj")).cast("Integer"))
  dfWithNewColumn.show()
  dfWithNewColumn.printSchema()
  // Se comprueba que la concatenación y el casting lo hace correctamente, primero concatena y luego
  // castea a entero

  /**
    * Añadir la resta/suma de dos columnas a un dataframe
    */

  val (df1,df2) = CreateDataframe.getNumberDF

  val input = df1.join(df2, df1("c1") === df2("c2"))

  val columnsToSum = List(col("c1"), col("c2"))

  val output =input.withColumn("sums", columnsToSum.reduce(_ + _))

  output.show(100, false)
*/

//  private val df =
//    Seq(1560, 1560, 1560, 1560, 1561, 1561, 1561, 1561, 1562, 1562, 1563)
//      .toDF("c1")
//
//  val dfWithPartitionDate = addDataToDfFromString(df, "xxx/yyy/.../intake_date=20191120")
//  dfWithPartitionDate.show
//
//
//  /**
//    * Se añade una columna con una fecha dada en formato xxx/yyy/.../intake_date=yyyyMMdd
//    * @param df
//    * @param date
//    */
//  def addDataToDfFromString(df: DataFrame, input: String): DataFrame = {
//    val partition = input.substring(input.lastIndexOf("/"), input.length).split("=").apply(1)
//    val sDate = partition.toString
//    val ssDate=  sDate.substring(0, 4)
//        .concat("-")
//        .concat(sDate.substring(4, 6))
//        .concat("-")
//        .concat(sDate.substring(6, 8))
//
//    val partialDf = df.withColumn("c2", lit(ssDate))
//    partialDf.show
//    partialDf.printSchema()
//    partialDf.select(to_date(col("c2"), "yyyy-MM-dd").as("c3"))
//  }
//
//  val longToStringDateUdf = udf(longToStringDate)
//  def longToStringDate: Long => String = date => {
//    val sDate = date.toString
//    sDate.substring(0, 4)
//      .concat("-")
//      .concat(sDate.substring(4, 6))
//      .concat("-")
//      .concat(sDate.substring(6, 8))
//  }
}
object AddDateColumnToDF extends App {

}
