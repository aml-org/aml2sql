package aml.sql

import scala.annotation.tailrec

trait Utils {

  val MAX_CHAR_SIZE = 500
  val PRIMARY_KEY_TYPE = "VARYING CHARACTER(36)"

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
}
