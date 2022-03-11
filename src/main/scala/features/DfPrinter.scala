package features

import org.apache.spark.sql.DataFrame

object DfPrinter {

  def showDfs(dfs: List[DataFrame]): Unit = dfs.foreach(_.show)
  def showAnPrintSchema(dfs: List[DataFrame]): Unit = {
    dfs.foreach(_.printSchema)
    showDfs(dfs)
  }

}
