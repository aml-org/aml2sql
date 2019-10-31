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
    val name = this.name.getOrElse(leftColumn)
    val candiate = s"${leftTable}_${name}_JOIN"
    if (candiate.length > 63) { // limit in Postgres
      val leftTableShrinked = leftTable.split("_").map(w => w.take(3).mkString).mkString("_")
      val nameShrinked = name.split("_").map(w => w.take(3).mkString).mkString("_")
      s"${leftTableShrinked}_${nameShrinked}_JOIN"
    } else {
      candiate
    }
  }

  def leftKey = s"${leftTable}_${leftColumn}"

  def rightKey = s"${rightTable}_${rightColumn}"

  def toObjectColumn(localForeignColumn: String, direction: String): Column = {
    val (propertyId, foreingKey, foreignNamespace, foreingTable) = direction match {
      case "left" => (leftProperty.get, rightColumn, rightNamespace, rightTable)
      case "right"=> (rightProperty.get, leftColumn, leftNamespace, leftTable)
      case _      => throw new Exception(s"Unknown join direction: '$direction': (left|right) supported")
    }
    Column(
      localForeignColumn,
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
