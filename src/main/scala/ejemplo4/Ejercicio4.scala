package ejemplo4

import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}
import java.util.regex.Pattern

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Ejercicio4 {

  def parse_apache_time(apache_time: String): Calendar = {

    val date = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH).parse(apache_time)

    val cal = Calendar.getInstance()
    cal.setTime(date)

    cal
  }

  case class LogRecord(host: String,
                       client_identd: String,
                       user_id: String,
                       date_time: Calendar,
                       method: String,
                       endpoint: String,
                       protocol: String,
                       response_code: Int,
                       content_size: Long)

  def parse_apache_log_line(logline: String): (LogRecord, Int) = {

    val LOG_ENTRY_PATTERN = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+)\\s*(\\S*) ?\" (\\d{3}) (\\S+)"
    val pattern = Pattern.compile(LOG_ENTRY_PATTERN)
    val matcher = pattern.matcher(logline)

    if (matcher.find())
      (LogRecord(
        matcher.group(1), // host
        matcher.group(2), // client_identd
        matcher.group(3), // user_id
        parse_apache_time(matcher.group(4)), // date_time
        matcher.group(5), // method
        matcher.group(6), // endpoint
        matcher.group(7), // protocol
        matcher.group(8).toInt, // response_code
        if (matcher.group(9) == "-") 0L else matcher.group(9).toLong) // content_size
        , 1)
    else
      (LogRecord(logline, "", "", null, "", "", "", 0, 0), 0)
  }

  def parse_logs(): RDD[LogRecord] = {

    // Spark Configuration Object
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Ejercicio4")

    val sc = new SparkContext(conf)

    val log_file = "data/apache.access.log"

    val parsed_logs = sc.textFile(log_file).map(parse_apache_log_line)
    val access_logs = parsed_logs.filter(x => x._2 == 1).map(x => x._1)
    val failed_logs = parsed_logs.filter(x => x._2 == 0).map(x => x._1)

    val failed_logs_count = failed_logs.count()
    if (failed_logs_count > 0) {
      println("Number of invalid logline:" + failed_logs_count)
      failed_logs.collect().foreach(println)
    }

    println("Read " + parsed_logs.count() + " lines, successfully parsed " + access_logs.count() +
      " lines, failed to parse " + failed_logs.count() + " lines")

    access_logs
  }
}
