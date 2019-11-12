package features

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

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
