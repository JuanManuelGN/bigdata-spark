package ejemplo4

import java.util.Calendar

import ejemplo4.Ejercicio4._
import org.apache.spark.rdd.RDD

/**
  * -- Mínimo, Máximo y Media del tamaño de las peticiones (size)
  * -- Nº peticiones de cada código de respuesta (response_code)
  * -- Mostrar 20 hosts que han sido visitados más de 10 veces
  * -- Mostrar los 10 endpoints más visitados
  * -- Mostrar los 10 endpoints más visitados que no tienen código de respuesta =200
  * -- Calcular el nº de hosts distintos
  * Contar el nº de hosts únicos cada día
  * -- Calcular la media de peticiones diarias por host
  * Mostrar una lista de 40 endpoints distintos que generan código de respuesta = 404
  * Mostrar el top 25 de endopoints que más códigos de respuesta 404 generan
  * El top 5 de días que se generaron código de respuestas 404
  */
object Funciones {
  def minMaxAvgContentSize(implicit rdd: RDD[LogRecord]) : (Long,Long,Double) = {
    val contentSize = rdd.map(l => l.content_size)
    (contentSize.max(),contentSize.min(),contentSize.mean())
  }
  // Nº peticiones de cada código de respuesta (response_code)
  def countResponse(implicit rdd: RDD[LogRecord]): Array[(Int, Int)] =
    rdd
      .map(l => (l.response_code, 1))
      .reduceByKey(_ + _)
      .collect()

  // Mostrar 20 hosts que han sido visitados más de 10 veces
  def hostMoreTen(implicit rdd: RDD[LogRecord]): Array[(String, Int)] =
    rdd
      .map(l => (l.host, 1))
      .reduceByKey(_ + _)
      .filter(t => t._2 > 10)
      .take(20)

  // Mostrar los 10 endpoints más visitados
  def tenEndpointMostVisited(implicit rdd: RDD[LogRecord]): Array[(String,Int)] =
    rdd
      .map(l => (l.endpoint, 1))
      .reduceByKey(_+_)
      .sortBy(t => t._2,false)
      .take(10)

  // Mostrar los 10 endpoints más visitados que no tienen código de respuesta =200
  def tenEndpointMostVisitedNo200(implicit rdd: RDD[LogRecord]): Array[(String,Int)] =
    rdd
      .filter(r => !r.response_code.equals(200))
      .map(l => (l.endpoint, 1))
      .reduceByKey(_+_)
      .sortBy(t => t._2,false)
      .take(10)

  // Calcular el nº de hosts distintos
  def distinctHost(implicit rdd: RDD[LogRecord]): Long =
    rdd.map(l => l.host).distinct().count()

  // Calcular la media de peticiones diarias por host
  def avgRequestPerHost(implicit rdd: RDD[LogRecord]) =
    rdd
      .map(l => (l.date_time.get(Calendar.DAY_OF_MONTH),l.host))
      .groupByKey().map(t => (t._1,t._2.size/t._2.toSet.size))
      .collect()
}

object MainEj4 extends App {
  implicit val access_logs = parse_logs()

  import Funciones._

  val resultList = List(
    s"Content size max ${minMaxAvgContentSize._1} " +
      s"Content size min ${minMaxAvgContentSize._2} " +
      s"Content size avg ${minMaxAvgContentSize._3}",
    countResponse.map(t => s"Response code ${t._1} counts ${t._2}"),
    hostMoreTen.map(t => s"Host ${t._1} count ${t._2}"),
    s"Number of distinct host $distinctHost",
    tenEndpointMostVisited.map(t => s"Endpoint ${t._1} counts ${t._2}"),
    tenEndpointMostVisitedNo200.map(t => s"Endpoint ${t._1} counts ${t._2}"),
    avgRequestPerHost.map(t => s"Day ${t._1} has average request per host ${t._2}")
  )

  resultList.foreach(l =>
    l match {
      case list:Array[String] => {
        list.toList.foreach(l => println(l))
        println("-------------------------------")
      }
      case s => {
        println(s)
        println("-------------------------------")
      }
    }
  )

  // PoC aplicación de funciones mediante List.map
  /*val functionList =
    List(
      countResponse,
      hostMoreTen
    )
  functionList.map(f => f(access_logs)).map(r => r.foreach(t => println(t)))*/

}