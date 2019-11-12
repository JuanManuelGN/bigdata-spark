package features

import config.SparkConfig

object Avg extends App with SparkConfig {

  import spark.implicits._

  /**
    * Calcular la media de los salarios agrupados por ciudad
    * El resultado es un DF con la localidad y la media del
    * salario que se cobra en dicha localidad.
    */
  val salary =
    Seq(
      ("Ram", 30, "Engineer", 40000),
      ("Bala", 27, "Doctor", 30000),
      ("Hari", 33, "Engineer", 50000),
      ("Siva", 35, "Doctor", 60000)).toDF("name", "age", "occupation", "salary")

  val location =
    Seq(
      ("Hari", "Bangalore"),
      ("Ram", "Chennai"),
      ("Bala", "Bangalore"),
      ("Siva", "Chennai")).toDF("name", "location")

  import org.apache.spark.sql.functions._

  salary
    .join(location,Seq("name"))
    .groupBy($"location")
    .agg(
      avg($"salary").as("avg_salary")
    ).show
}
