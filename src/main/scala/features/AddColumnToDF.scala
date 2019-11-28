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

  def addLongColConvertToDateWithUdf(df: DataFrame): DataFrame = {
    val dfAdded = df.withColumn("colDateLong", lit(null))
    val dfAddedWithStringCol = dfAdded.withColumn("colStringDate", longToStringDateUdf(col("colDateLong")))
    dfAddedWithStringCol.withColumn("colDate", to_date(col("colStringDate"), "yyyy-MM-dd"))
  }
  val longToStringDateUdf = udf(longToStringDate)
  def longToStringDate: Long => String = date => {
    val sDate = date.toString
    sDate.substring(0, 4)
      .concat("-")
      .concat(sDate.substring(4, 6))
      .concat("-")
      .concat(sDate.substring(6, 8))
  }
}
object AddColumnToDFWithUdfFunction extends App {

  import spark.implicits._

  val cardsDF = CreateDataframe.getCardsDF

  val addedUdfColumn = AddColumnToDF().addColumnWithUdfFunction(cardsDF)

  addedUdfColumn.show
}
object AddDateColumnToDF extends App {

}
object AddLongDateColumUdf extends App {

  val df = CreateDataframe.getTimeUnixDf
  val response = AddColumnToDF().addLongColConvertToDateWithUdf(df)
  response.show
}
