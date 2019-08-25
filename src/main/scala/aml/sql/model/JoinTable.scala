package aml.sql.model

import aml.sql.Utils

case class JoinTable(namespace: String,
                     name: Option[String],
                     leftTable: String,
                     leftColumn: String,
                     leftCardinality: Option[Int],
                     leftProperty: Option[String],
                     rightTable: String,
                     rightColumn: String,
                     rightCardinality: Option[Int],
                     rightProperty: Option[String]) extends Utils {
  val tableName: String = {
    val column = name.getOrElse("JOIN")
    if (column.endsWith(rightTable)) {
      s"""${leftTable}_$column"""
    } else {
      s"${leftTable}_${column}_${rightTable}"
    }
  }
}
