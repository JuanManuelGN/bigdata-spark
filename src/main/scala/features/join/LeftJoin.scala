package features.join

import config.SparkConfig
import org.apache.spark.sql.DataFrame

case class LeftJoin() {

  def leftJoin(dfs: DataFrame*): DataFrame =
    dfs.reduceLeft(_.join(_, Seq("c1"), "left"))
}

object LeftJoin extends SparkConfig with App {

  import spark.implicits._

  val df1 = Seq(
    (0, "0"),
    (1, "1"),
    (2, "2")
  ).toDF("c1", "c2")

  val df2 = Seq(
    (0, "cero"),
    (0, "tres"),
    (1, "uno"),
    (2, "dos")
  ).toDF("c1", "c2")

  val dfJoined = LeftJoin().leftJoin(List(df1, df2): _*)

  dfJoined.show
}
