package aml.sql

import amf.plugins.document.vocabularies.model.domain.PropertyMapping

import scala.annotation.tailrec

trait Utils {

  val MAX_CHAR_SIZE = 500
  val PRIMARY_KEY_TYPE = "VARCHAR(36)"

  def capitalize(id: String): String = {
    val base = id.replaceAll(" ", "_")
    val camelCased = camel2Snake(base)
    camelCased.split("_").filter(_ != "").map(_.toUpperCase).mkString("_")
  }

  protected def camel2Snake(str: String): String = {
    @tailrec
    def camel2SnakeRec(s: String, output: String, lastUppercase: Boolean): String =
      if (s.isEmpty) output
      else {
        val c = if (s.head.isUpper && !lastUppercase) "_" + s.head.toLower else s.head.toLower
        camel2SnakeRec(s.tail, output + c, s.head.isUpper && !lastUppercase)
      }
    if (str.forall(_.isUpper)) str.map(_.toLower)
    else {
      camel2SnakeRec(str, "", true)
    }
  }

  def xsdToSql(str: String, propertyMapping: PropertyMapping): String = {
    str match {
      case "http://www.w3.org/2001/XMLSchema#hexBinary" => "BINARY"
      case "http://www.w3.org/2001/XMLSchema#decimal"   => "DECIMAL"
      case "http://www.w3.org/2001/XMLSchema#integer"   => "INTEGER"
      case "http://www.w3.org/2001/XMLSchema#double"    => "FLOAT"
      case "http://www.w3.org/2001/XMLSchema#boolean"   => "BOOLEAN"
      case "http://www.w3.org/2001/XMLSchema#date"      => "DATE"
      case "http://www.w3.org/2001/XMLSchema#time"      => "TIME"
      case "http://www.w3.org/2001/XMLSchema#dateTime"  => "TIMESTAMP"
      case "http://www.w3.org/2001/XMLSchema#string"    =>
        propertyMapping.maximum().option match {
          case Some(max) => s"VARCHAR(${max.toInt})"
          case _         => s"VARCHAR($MAX_CHAR_SIZE)"
        }
      case _               => s"VARCHAR($MAX_CHAR_SIZE)"
    }
  }

  def sqlToXsd(str: String): String = {
    str match {

      case "BINARY"	   => "http://www.w3.org/2001/XMLSchema#hexBinary"
      case "DECIMAL"	 => "http://www.w3.org/2001/XMLSchema#decimal"
      case "INTEGER"	 => "http://www.w3.org/2001/XMLSchema#integer"
      case "FLOAT"	   => "http://www.w3.org/2001/XMLSchema#double"
      case "BOOLEAN"	 => "http://www.w3.org/2001/XMLSchema#boolean"
      case "DATE"	     => "http://www.w3.org/2001/XMLSchema#date"
      case "TIME"	     => "http://www.w3.org/2001/XMLSchema#time"
      case "TIMESTAMP" => "http://www.w3.org/2001/XMLSchema#dateTime"
      case _		       => "http://www.w3.org/2001/XMLSchema#string"
    }
  }
}
