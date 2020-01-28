package features

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, when}

object Join extends App {

  val spark = SparkSession.builder()
    .appName("Join")
    .config("spark.master", "local")
    .getOrCreate()

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val left =
    Seq(
      (0, "la", "la"),
      (0, "lo", null),
      (1, "one", "uno"))
      .toDF("id", "left", "Spanish")

  val right =
    Seq(
      (0, "zero", "cero"),
      (0, "zero", null),
      (2, "two", "dos"),
      (3, "three", "tres"))
      .toDF("id", "right", "Spanish")

//  println("inner")
//  left.join(right, "id").show
//
//  println("fullouter")
//  left.join(right, Seq("id"), "fullouter").show
//  println("fullouter with id and Spanish")
//  left.join(right, Seq("id","Spanish"), "fullouter").show

  right.groupBy("id", "Spanish").agg(count("id").as("activated_number")).show()

  println("outer with id and Spanish")
  left
    .withColumnRenamed("Spanish", "SpanishLeft")
    .join(right.withColumnRenamed("Spanish", "SpanishR"), Seq("id"), "outer")
    .withColumn("SpanishJunto", coalesce($"SpanishLeft",$"SpanishR"))
    .drop("SpanishLeft")
    .drop("SpanishR")
//    .groupBy("id")
//    .agg(count("id").as("new_number"))
    .show
//  println("left with id and Spanish")
//  left.join(right, Seq("id", "Spanish"), "left").show
//  println("LeftOuter with id")
//  left.join(right, Seq("id"), "LeftOuter").show
//  println("LeftOuter with id and Spanish")
//  left.join(right, Seq("id", "Spanish"), "LeftOuter").show

//  duplicates(left, right)

  leftJoinwitchLeftMinusRigth.show

  /**
    * Join con identificadores duplicados
    */
    def duplicates(left: DataFrame, right: DataFrame): Unit = {

      val duplicateLeft = left.select("id", "left")
      val duplicateRight = right.select("id", "right")

      val duplicates = duplicateLeft.join(duplicateRight, Seq("id"), "left")

      duplicates.show
    }

  /**
    * Left join con dataframe left con menor número de registros que el dataframe rigth
    */
  def leftJoinwitchLeftMinusRigth: DataFrame ={
    val left =
      Seq(
        (0, "la", "la"),
        (1, "lo", null),
        (2, "one", "uno"))
        .toDF("id", "left", "Spanish")

    val right =
      Seq(
        (0, "zero", "cero"),
        (1, "zero", null),
        (2, "two", "dos"),
        (3, "three", "tres"))
        .toDF("id", "right", "Spanish")

    left.join(right, Seq("id"), "full")
  }
}

case class JoinWithEmptyDf() {
  def doJoin(df: DataFrame, emptyDf: DataFrame): DataFrame =
    df.join(emptyDf, Seq("id"), "left_anti")
}
object JoinWithEmptyDf extends App {
  val df = CreateDataframe.getIntDf
  val emptyDf = CreateDataframe.getEmptyDf

  emptyDf.show

  val dfJoined = JoinWithEmptyDf().doJoin(df, emptyDf)

  dfJoined.show

}

case class JoinAndSum() {
  def joinAndSum: DataFrame => DataFrame => DataFrame = df1 => df2 => {
    df1.join(df2, Seq(df1.columns.head))
      .withColumn("sum", List(col(df1.columns(1)), col(df2.columns(1))).reduce(_ + _))
  }
}
object JoinAndSum extends App {
  val df1 = CreateDataframe.getJoinAndSum1
  val df2 = CreateDataframe.getJoinAndSum2
  val df2Renamed = df2.withColumnRenamed(df2.columns(1), df2.columns(1) + "_")
  val response = JoinAndSum().joinAndSum(df1)(df2Renamed)
  response.show
}

/**
  * dados dos ddfs que se tiene que unir, teniendo en cuenta que el campo por el que se
  * deben unir difiere en uno de sus valores, PTE y PENDING. Estos valores tienen el
  * mismo significado porque lo que debería de dar positivo en una comparación, es decir,
  * PTE = PENDING.
  * Lo que se va a hacer es modificar el df que contiene en un columna PTE y cambiar todos
  * los PTE por PENDING para luego hacer el join
  */
object JoinAndSustituteValue extends App {
  val (lookup, df) = CreateDataframe.joinAndSustituteValueDf
  val response =
    df.select(col("col11"),
      when(col("col22") === "PTE", "PENDING")
        .otherwise(col("col22")).as("col22"),
      col("col33"))
      .join(lookup, col("col22") === col("col2"))
      .select("col11", "col3")
  response.show
}