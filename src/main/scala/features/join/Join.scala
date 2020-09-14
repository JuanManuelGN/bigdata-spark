package features.join

import config.SparkConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

case class Join() {

  def innerJoin(dfs: DataFrame*): DataFrame =
    dfs.reduceLeft(_.join(_, dfs.map(df => df.columns.head), "inner"))

  def leftJoin(dfs: DataFrame*): DataFrame =
    dfs.reduceLeft(_.join(_, Seq("c1"), "left"))

  def rightJoin(dfs: DataFrame*): DataFrame =
    dfs.reduceLeft(_.join(_, Seq("c1"), "right"))
}

object Join extends SparkConfig with App {

  import spark.implicits._

  val df1 = Seq(
    (0, "0"),
    (1, "1"),
    (2, "2"),
    (3, "3")
  ).toDF("id", "df1_c2")

  val df2 = Seq(
    (0, "cero"),
    (1, "uno"),
    (2, "dos"),
    (4, "cuatro")
  ).toDF("id", "df2_c2")

//  val leftJoinDf = Join().leftJoin(List(df1, df2): _*)
//  leftJoinDf.show

//  val rightJoinDf = Join().rightJoin(List(df1, df2): _*)
//  rightJoinDf.show

  val innerJoinDf = Join().innerJoin(List(df1, df2, df1): _*)
  innerJoinDf.show
}
