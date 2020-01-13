package utils

import java.io.File

import org.apache.spark.sql.{DataFrame, SparkSession}

object TestingUtils extends ResourceUtils {

  /**
    * Resource directory that holds the expected lookup tables after
    * processing the appian samples defined in `AppianSamplesResourceDir`
    */
  val ExpectedLookupsResourceDir = "/data/expected/"

  /**
    * Load the expected lookup tables data from /resources/expected
    * @param spark spark
    * @return map of tableName, dataFrame
    */
  def loadExpectedLookupDfs()(implicit spark: SparkSession): Map[String, DataFrame] = {
    getDirectoryDataFrames(ExpectedLookupsResourceDir).map {
      case (fileName, df) =>
        val tableName = fileName.toUpperCase.substring(0, fileName.lastIndexOf("."))
        (tableName, df)
    }
  }

  /**
    * Get a map of filename => dataframe given a path
    * @param path path
    * @param spark spark
    * @return map of filename => dataframe given a path
    */
  def getDirectoryDataFrames(path: String)
                            (implicit spark: SparkSession): Map[String, DataFrame] = {
    val folder = new File(getResourceFullPath(path))
    folder.listFiles().map(file => {
      val dataFrame = spark
        .read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(file.getPath)
      (file.getName, dataFrame)
    }).toMap
  }
}
