package features.columnFunctions

import config.SparkConfig
import features.DataframeFunctions
import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.sql.types.IntegerType

object Split extends SparkConfig with App with DataframeFunctions{

  import spark.implicits._

  val df = Seq("0.0", "1.1", "2.1", "3.1").toDF("c1")

  val splitDf = df.withColumn("split", split(col("c1"), "\\git push."))

//  showAnPrintSchema(List(df, splitDf))

  val df2 = Seq("\"4\"", "4").toDF("n")
  df2.show(false)
  df2.printSchema()
  val df3 = df2.withColumn("casting", df2("n").cast(IntegerType))
  df3.printSchema()
  df3.show(false)
}
