package features

import features.Rows.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType


case class Nulls() {
  def cast(dfWithNulls: DataFrame): Unit = {
    dfWithNulls.show
    println(
      dfWithNulls.select("field2").head.getString(0) == null)
  }
}
object Nulls extends App {
  import spark.implicits._

  val df = CreateDataframe.getIncomingDf

  Nulls().cast(df)
}

case class Decimal() {
  def cast: DataFrame => DataFrame = df =>
    df.select(col("col1").cast(DecimalType(10, 3)).as("casted"))
      .withColumn("col2", col("casted"))
}
object Decimal extends App {

  val decimalDf = CreateDataframe.getDecimalDf
  val df = Decimal().cast(decimalDf)
  df.printSchema
  df.show
}

object Rows extends App {

  val spark = SparkSession.builder()
    .appName("Join")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val left =
    Seq(
      (0, "zero", null),
      (1, "one", "uno"))
      .toDF("id", "left", "Spanish")

  val right =
    Seq(
      (0, "zero", "cero"),
      (0, "zero", null),
      (2, "two", "dos"),
      (3, "three", "tres"))
      .toDF("id", "right", "Spanish")

  val emptyDF = Seq((0),(9)).toDF("c0")

  // Seleccionar la primera fila del dataframe
  val firstRowLeft = left.select("*").first()
  println(s"Primera fila $firstRowLeft")

  // Seleccionar el segundo componente de la primera fila del dataframe
  val secondComponentOfFirstRow = firstRowLeft.get(1)
  println(s"Segundo componente de la primera fila $secondComponentOfFirstRow")

  // Seleccionar un componente que no se encuentra, por ejemplo el cuarto, primero
  // se comprueba si existe o no
  val notFoundComponent = firstRowLeft.isNullAt(0)
  println(notFoundComponent)

  /**
    * Transformar una secuencia con una única fila en un dataframe y obtener dicha
    * fila
    */
  println(
    Seq((0L,0L)).toDF().first()
  )
}
