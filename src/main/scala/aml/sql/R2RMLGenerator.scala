package aml.sql

import aml.sql.model.{Column, DataBase, JoinTable, Table}
import io.circe.Json

class R2RMLGenerator(namespace: String = "http://cim.org/instances#", database: DataBase) extends Utils {

  def generate(): String = {
    val mappedTables = database.tables.map { table =>
      mapTable(table)
    }
    val mappedJoinTables = database.joinTables.map { joinTable =>
      mapJoinTable(joinTable)
    }
    jobj(Seq(
      "@context" -> jobj(Seq(
        "rr" -> jstr("http://www.w3.org/ns/r2rml#")
      )),
      "@graph" -> jarray(
        mappedTables ++ mappedJoinTables
      )
    )).toString()
  }

  protected def mapTable(table: Table): Json = {
    val scalarMappings: Seq[Json] = scalarColumns(table).map(mapScalarColumn)
    val objectMappings: Seq[Json] = objectColumns(table).map(mapObjectColumn)
    jobj(Seq(
      "@id" -> jstr(tableId(table)),
      "rr:logicalTable" ->  jobj(Seq(
        "rr:tableName" -> jstr( s"${table.namespace}.${table.name}")
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

  protected def mapJoinTable(joinTable: JoinTable): Json = {
    if (joinTable.leftProperty.isDefined) {
      jobj(Seq(
        "@id" -> jstr(joinTableId(joinTable)),
        "rr:logicalTable" ->  jobj(Seq(
          "rr:tableName" -> jstr( s"${joinTable.namespace}.${joinTable.tableName}")
        )),
        "rr:subjectMap" -> jobj(Seq(
          "rr:template" -> jstr(cim(joinTable.leftColumn))
        )),
        "rr:predicateObjectMap" -> mapObjectColumn(joinTable.toObjectColumn("left"))
      ))
    } else {
      jobj(Seq(
        "@id" -> jstr(joinTableId(joinTable)),
        "rr:logicalTable" ->  jobj(Seq(
          "rr:tableName" -> jstr( s"${joinTable.namespace}.${joinTable.name}")
        )),
        "rr:subjectMap" -> jobj(Seq(
          "rr:template" -> jstr(cim(joinTable.rightColumn))
        )),
        "rr:predicateObjectMap" -> mapObjectColumn(joinTable.toObjectColumn("right"))
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
      "rr:predicate" -> jstr(column.propertyId),
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
    val parentTable = database.tables.find(_.name == column.foreignTable.get).getOrElse(throw new Exception(s"Missing parent table for column '${column.name}"))
    jobj(Seq(
      "rr:predicate" -> jstr(column.propertyId),
      "rr:objectMap" -> jobj(Seq(
        "rr:parentTripleMap" -> jobj(Seq(
          "@id" -> jstr(tableId(parentTable))
        )),
        "rr:joinCondition" -> jobj(Seq(
          "rr:parent" -> jstr(column.foreignKey.getOrElse(s"Missing foreign key mapping for column ${column.name}")),
          "rr:child" -> jstr(column.foreignKey.getOrElse(s"Missing foreign key mapping for column ${column.name}"))
        ))
      ))
    ))
  }


  protected def tableId(table: Table): String = s"/${table.namespace}/${table.name}"
  protected def joinTableId(joinTable: JoinTable): String = s"/${joinTable.namespace}/${joinTable.tableName}"

  def jobj(fs: Seq[(String, Json)]): Json = Json.fromFields(fs)
  def jstr(str: String): Json = Json.fromString(str)
  def jarray(values: Seq[Json]): Json = Json.fromValues(values)
  def cim(str: String) = s"$namespace{$str}"
}