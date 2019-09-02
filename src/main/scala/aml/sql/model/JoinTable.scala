package aml.sql.model

import aml.sql.utils.Utils

case class JoinTable(leftNamespace: String,
                     name: Option[String],
                     leftTable: String,
                     leftColumn: String,
                     leftCardinality: Option[Int],
                     leftProperty: Option[String],
                     rightNamespace:String,
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

  def toObjectColumn(direction: String): Column = {
    val (name, propertyId, foreingKey, foreignNamespace, foreingTable) = direction match {
      case "left" => (leftColumn, leftProperty.get, rightColumn, rightNamespace, rightTable)
      case "right"=> (rightColumn, rightProperty.get, leftColumn, leftNamespace, leftTable)
      case _      => throw new Exception(s"Unknown join direction: '$direction': (left|right) supported")
    }
    Column(
      name,
      key = false,
      required = false,
      propertyId,
      None,
      Some(foreingKey),
      Some(foreignNamespace),
      Some(foreingTable)
    )
  }

}
