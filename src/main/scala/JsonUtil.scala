package com.etsy.sahale


import spray.json._
import DefaultJsonProtocol._


object JsonUtil {
  def encode(any: Any): JsValue = any match {
    case null               => JsNull
    case b: Boolean         => JsBoolean(b)
    case l: Long            => JsNumber(l)
    case i: Int             => JsNumber(i)
    case d: Double          => JsNumber(d)
    case f: Float           => JsNumber(f)
    case s: String          => if (null == s) JsNull else JsString(s)
    case _                  =>
      throw new IllegalArgumentException(s"Bad value passed to JsonUtil#encode: ${any}")
  }

  def toJsonMap(map: Map[String, Any]): Map[String, JsValue] = {
    map.foldLeft(Map.empty[String, JsValue]) { (out, entry) =>
      entry._2 match {
        case s: Seq[Any]         @unchecked => out ++ Map(entry._1 -> JsArray(s.toList.map{encode(_)}))
        case m: Map[String, Any] @unchecked => out ++ Map(entry._1 -> toJsonMap(m).toJson)
        case a: Any                         => out ++ Map(entry._1 -> encode(a))
        case _                              => out
      }
    }
  }

  // quick and dirty, only for use with job progress values (0.00%-100.00%)
  def percent(d: Double): Double = ("%3.2f" format (d * 100.0)).toDouble
}
