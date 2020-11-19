package features

import config.SparkConfig
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

trait ColumnFunction {

  def rlike(c: Column, literal: String): Column = c.rlike(literal)

}

object ColumnFunctionRunner extends SparkConfig with App with DataframeFunctions with ColumnFunction {
  import spark.implicits._

  val df = Seq("0", "1", "2", "3").toDF("c1")
  val response = df.withColumn("c2", rlike(df("c1"), "1"))

//  showAnPrintSchema(List(df, response))
  /**
    * +---+-----+
    * | c1|   c2|
    * +---+-----+
    * |  0|false|
    * |  1| true|
    * |  2|false|
    * |  3|false|
    * +---+-----+
    */

  val df1 = Seq("Hola mundo", "Hola", "Hasta luego").toDF("c1")
  val response1 = df1.withColumn("c2", rlike(df1("c1"), "Hola"))
//  showAnPrintSchema(List(df1, response1))
  /**
    * +-----------+-----+
    * |         c1|   c2|
    * +-----------+-----+
    * | Hola mundo| true|
    * |       Hola| true|
    * |Hasta luego|false|
    * +-----------+-----+
    */

  val df2 = Seq("UnoDos", "UnoCuatro", "Uno").toDF("c1")
  val response2 = df2.withColumn("c2", rlike(df2("c1"), "Uno"))
//  showAnPrintSchema(List(df2, response2))
  /**
    * +---------+----+
    * |       c1|  c2|
    * +---------+----+
    * |   UnoDos|true|
    * |UnoCuatro|true|
    * |      Uno|true|
    * +---------+----+
    */

  /**
    * Un ejemplo donde dado un dataframe con una columna id y otra columna con un texto, se va a
    * hacer un group by por el id y establecer en función del texto un valor numérico
    */
  val Uno = "Uno"
  val Seis = "seis"
  val Ocho = "ocho"
  val literals: String = List(Uno, Seis).mkString("|")
  val df3 = Seq(
    (1, "UnoDos"),
    (2, "UnoCuatro"),
    (1, "Tres"),
    (2, "cinco"),
    (3, "seis"),
    (4, null),
    (4, "ocho"),
    (5, null),
    (6, "catorce"))
    .toDF("id", "c1")
  val grupDF =
    df3.groupBy("id")
      .agg(
        first(
          when(df3("c1").rlike(literals), 1)
            .when(df3("c1").rlike(Ocho), 2)
            .otherwise(0)
        ).as("status")
      ).orderBy("id")
//  showAnPrintSchema(List(df3, grupDF))
  /**
    * Con la función first
    * +---+------+
    * | id|status|
    * +---+------+
    * |  1|     1|
    * |  2|     1|
    * |  3|     1|
    * |  4|     0| Debería ser 2, pero ha evaluado antes el otherwise
    * |  5|     0|
    * |  6|     0|
    * +---+------+
    */

  val grupDF2 =
    df3.groupBy("id")
      .agg(
        first(
          when(df3("c1").rlike(literals), 1)
            .when(df3("c1").rlike(Ocho), 2)
//            .otherwise(0)
        ).as("status")
      ).orderBy("id")
//  showAnPrintSchema(List(df3, grupDF2))
  /**
    * Parece que con unos when anidados el proceso es incapaz de
    * computar el segundo when, creo que por la función first.
    * +---+------+
    * | id|status|
    * +---+------+
    * |  1|     1|
    * |  2|     1|
    * |  3|     1|
    * |  4|  null| Debería ser 2
    * |  5|  null|
    * |  6|  null|
    * +---+------+
    */

  /**
    * Con collect_set
    */
  val grupDF3 =
    df3.groupBy("id")
      .agg(
        collect_set("c1").as("status")
      )
  /**
    * +---+------------------+
    * | id|            status|
    * +---+------------------+
    * |  1|    [Tres, UnoDos]|
    * |  6|         [catorce]|
    * |  3|            [seis]|
    * |  5|                []|
    * |  4|            [ocho]|
    * |  2|[cinco, UnoCuatro]|
    * +---+------------------+
    */
  val field =
    grupDF3.withColumn("field",
      when(col("status").rlike(Uno), 1)
        .when(array_contains(col("status"), Seis), 1)
      .when(array_contains(col("status"), Ocho), 2)
      .otherwise(0))
      .orderBy("id")


//        col("status").formatted().rlike(literals), 1)
//        .when(col("status").rlike(Ocho), 2)
//        .otherwise(0))
    showAnPrintSchema(List(grupDF3, field))
  /**
    * +---+------------------+-----+
    * | id|            status|field|
    * +---+------------------+-----+
    * |  1|    [Tres, UnoDos]|    0|
    * |  2|[cinco, UnoCuatro]|    0|
    * |  3|            [seis]|    1|
    * |  4|            [ocho]|    2|
    * |  5|                []|    0|
    * |  6|         [catorce]|    0|
    * +---+------------------+-----+
    */

}
