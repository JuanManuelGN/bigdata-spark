package features.columnFunctions

import features.Executable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, lower, when}

object IsIn extends Executable {

  import spark.implicits._

  val df: DataFrame = Seq("Málaga", "Sevilla", "Córdoba", "Cadiz").toDF("city")

  val filteredByCityDf = df.filter(col("city").isin(List("Málaga", "Cadiz"): _*))

  filteredByCityDf.show()

  /**
    * Dado una lista de posibles valores, comprobar si los valores en una columna de un
    * dataframe están incluidos el la lista con las siguientes restricciones:
    * 1. No importa las mayúsculas ni las minúsculas
    * 2. Si en la columna tenemos el valor null o el vacío ("") se establece el valor por
    *    defecto Other
    * @return
    */
  def isInLowerCaseWithDefault: DataFrame = {
    val roadTypeColName = "roadType"
    val roadTypes = List("Other", "Residential", "LivingStreet")
    val roadTypesWithoutOther = List("Residential", "LivingStreet")

    val df: DataFrame = Seq("RESIDENTIAL", "", null, "LivingStreet").toDF("roadType")

    val roadTypeLowerWithDefault =
      lower(
        when(col(roadTypeColName).isNull || col(roadTypeColName) === "", lit("Other"))
          .otherwise(col(roadTypeColName))
      ).as("roadTypeLowerWithDefault")

    val projection = Seq(
      col(roadTypeColName),
      roadTypeLowerWithDefault,
      when(roadTypeLowerWithDefault.isin(roadTypesWithoutOther.map(_.toLowerCase): _*), "Included")
        .otherwise("Excluded")
        .as("result")
    )

    df.select(projection: _*)
  }

  isInLowerCaseWithDefault.show(false)


}