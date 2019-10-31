package aml.sql

import aml.sql.model.{Column, DataBase, JoinTable, Table}
import aml.sql.utils.Utils
import io.circe.Json

class R2RMLGenerator(namespace: String = "http://cloudinformationmodel.org/instances#", database: DataBase) extends Utils {

  def generate(global: Boolean = false): String = {
    val mappedTables = database.tables.map { table =>
      mapTable(table, global)
    }
    val mappedJoinTables = database.joinTables.map { joinTable =>
      mapJoinTable(joinTable, global)
    }

    if (global) {
      jarray(mappedTables ++ mappedJoinTables).toString()
    } else {
      jobj(Seq(
        "@context" -> jobj(Seq(
          "rr" -> jstr("http://www.w3.org/ns/r2rml#")
        )),
        "@graph" -> jarray(
          mappedTables ++ mappedJoinTables
        )
      )).toString()
    }
  }

  protected def mapTable(table: Table, global: Boolean = false): Json = {
    val scalarMappings: Seq[Json] = scalarColumns(table).map(mapScalarColumn)
    val objectMappings: Seq[Json] = objectColumns(table).map(mapObjectColumn)
    val prefix = if (global) { "" } else { s"${table.namespace}." }
    jobj(Seq(
      "@id" -> jstr(tableId(table)),
      "rr:logicalTable" ->  jobj(Seq(
        "rr:tableName" -> jstr( s"${prefix}${table.name}")
      )),
      "rr:subjectMap" -> jobj(Seq(
        "rr:template" -> jstr(cim(table.keyColumn)),
        "rr:class" -> jobj(Seq(
          "@id" -> jstr(table.classId)
        ))
      )),
      "rr:predicateObjectMap" -> jarray(scalarMappings ++ objectMappings)
    ))
  }

  protected def mapJoinTable(joinTable: JoinTable, global: Boolean = false): Json = {
    var leftKey = joinTable.leftKey
    var rightKey = joinTable.rightKey

    if (leftKey == rightKey) { // recursive table
      leftKey = leftKey + "_LEFT"
      rightKey = rightKey + "_RIGHT"
    }


    val leftPrefix = if (global) { "" } else { s"${joinTable.leftNamespace}." }

    if (joinTable.leftProperty.isDefined) {
      jobj(Seq(
        "@id" -> jstr(joinTableId(joinTable)),
        "rr:logicalTable" ->  jobj(Seq(
          "rr:tableName" -> jstr( s"${leftPrefix}${joinTable.tableName}")
        )),
        "rr:subjectMap" -> jobj(Seq(
          "rr:template" -> jstr(cim(leftKey))
        )),
        "rr:predicateObjectMap" -> mapObjectColumn(joinTable.toObjectColumn(rightKey, "left"))
      ))
    } else {
      jobj(Seq(
        "@id" -> jstr(joinTableId(joinTable)),
        "rr:logicalTable" ->  jobj(Seq(
          "rr:tableName" -> jstr( s"${leftKey}.${joinTable.name}")
        )),
        "rr:subjectMap" -> jobj(Seq(
          "rr:template" -> jstr(cim(rightKey))
        )),
        "rr:predicateObjectMap" -> mapObjectColumn(joinTable.toObjectColumn(leftKey, "right"))
      ))
    }

  }

  protected def scalarColumns(table: Table): Seq[Column] = {
    table.columns.filter { column =>
      column.name != table.keyColumn && column.foreignKey.isEmpty
    }
  }

  protected def mapScalarColumn(column: Column): Json = {
    jobj(Seq(
      "rr:predicate" -> jobj(Seq(
        "@id" -> jstr(column.propertyId)
      )),
      "rr:objectMap" -> jobj(Seq(
        "rr:column" -> jstr(column.name),
        "rr:datatype" -> jobj(Seq(
          "@id" -> jstr(sqlToXsd(column.dataType.get))
        ))
      ))
    ))
  }

  protected def objectColumns(table: Table): Seq[Column] = {
    table.columns.filter { column =>
      column.name != table.keyColumn && column.foreignKey.nonEmpty
    }
  }

  protected def mapObjectColumn(column: Column): Json = {
    jobj(Seq(
      "rr:predicate" -> jobj(Seq(
        "@id" -> jstr(column.propertyId)
      )),
      "rr:objectMap" -> jobj(Seq(
        "rr:parentTriplesMap" -> jobj(Seq(
          "@id" -> jstr(tableId(column.foreignNamespace.get, column.foreignTable.get))
        )),
        "rr:joinCondition" -> jobj(Seq(
          "rr:child" -> jstr(column.name),
          "rr:parent" -> jstr(column.foreignKey.getOrElse(s"Missing foreign key mapping for column ${column.name}"))
        ))
      ))
    ))
  }


  protected def tableId(table: Table): String = tableId(table.namespace, table.name)
  protected def tableId(namespace: String, name: String): String = s"/${namespace}/${name}".toLowerCase()
  protected def joinTableId(joinTable: JoinTable): String = s"/${joinTable.leftNamespace}/${joinTable.tableName}".toLowerCase()

  def jobj(fs: Seq[(String, Json)]): Json = Json.fromFields(fs)
  def jstr(str: String): Json = Json.fromString(str)
  def jarray(values: Seq[Json]): Json = Json.fromValues(values)
  def cim(str: String) = s"$namespace{$str}"
}
