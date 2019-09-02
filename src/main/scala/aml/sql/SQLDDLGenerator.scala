package aml.sql

import aml.sql.model.{Column, DataBase, JoinTable, Table}
import aml.sql.utils.Utils

object SQLDDLGenerator extends Utils {

  // Generates a self contained schema that replaces existing tables
  def generate(database: DataBase): String = {
    val tables = database.tables.map(tableDDL)
    val joinTables = database.joinTables.map(joinTableDDL)
    schemasDDL(database) + "\n\n" + (tables ++ joinTables).mkString("\n\n")
  }
  ////////////////////////

  // Functions used to generate the global schema
  // taking into account dependencies among tables
  def generateDeclarationSchemas(database: DataBase): String = schemasDDL(database)
  def generateDefinitionTables(database: DataBase): String = {
    database.tables.map(tableDefinitionDDL).mkString("\n\n")
  }
  def generateTableConstraints(database: DataBase): String = {
    database.tables.map(tableConstraintDDL).mkString("\n\n")
  }
  def generateDefinitionJoinTables(database: DataBase): String = {
    database.joinTables.map(joinTableDDL).mkString("\n\n")
  }

  ////////////////////////

  protected def schemasDDL(database: DataBase): String = {
    database.schemas.map { schema =>
      s"CREATE SCHEMA IF NOT EXISTS ${schema};"
    } mkString("\n")
  }

  protected def joinTableDDL(joinTable: JoinTable): String = {
    val start = s"CREATE TABLE ${joinTable.leftNamespace}.${joinTable.tableName} ("
    val leftColumn = s"  ${joinTable.leftColumn}  ${PRIMARY_KEY_TYPE} NOT NULL"
    val rightColumn = s"  ${joinTable.rightColumn} ${PRIMARY_KEY_TYPE} NOT NULL"
    val leftConstraint = s"  FOREIGN KEY(${joinTable.leftColumn}) REFERENCES ${joinTable.leftNamespace}.${joinTable.leftTable}(${joinTable.leftColumn})"
    val rightConstraint = s"  FOREIGN KEY(${joinTable.rightColumn}) REFERENCES ${joinTable.rightNamespace}.${joinTable.rightTable}(${joinTable.rightColumn})"
    val end = ");";

    val definition = Seq(leftColumn, rightColumn, leftConstraint, rightConstraint).mkString(",\n")
    Seq(start, definition, end).mkString("\n")
  }

  protected def tableDefinitionDDL(table: Table): String = {
    val start = s"CREATE TABLE IF NOT EXISTS ${table.namespace}.${table.name} ("
    val nameLength = table.columns.map(_.name.length).max
    val typeLength = table.columns.map(c => c.dataType.orElse(c.foreignKey)).collect{case Some(r: String) => r.length }.max

    var columns = table.columns.map { column =>
      columnDDL(column, nameLength + 4, typeLength + 4)
    }
    val declaration = columns.mkString(",\n")
    val end = ");"

    Seq(start, declaration, end).mkString("\n")
  }

  protected def tableConstraintDDL(table: Table): String = {
    val start = s"ALTER TABLE IF EXISTS ${table.namespace}.${table.name}"

    val constraints = Seq(keyColumnDDL(table)) ++ table.columns.filter(_.foreignKey.isDefined).map(foreignKeyDDL)

    val declaration = constraints.map("ADD " + _).mkString(",\n")
    val end = ";"

    if (constraints.nonEmpty)
      Seq(start, declaration, end).mkString("\n")
    else
      ""
  }

  protected def tableDDL(table: Table): String = {
    val start = s"CREATE TABLE ${table.namespace}.${table.name} ("
    val nameLength = table.columns.map(_.name.length).max
    val typeLength = table.columns.map(c => c.dataType.orElse(c.foreignKey)).collect{case Some(r: String) => r.length }.max

    var columns = table.columns.map { column =>
      columnDDL(column, nameLength + 4, typeLength + 4)
    }
    var constraints = Seq(keyColumnDDL(table)) ++ table.columns.filter(_.foreignKey.isDefined).map(foreignKeyDDL)

    val declaration = (columns ++ constraints).mkString(",\n")
    val end = ");"

    Seq(start, declaration, end).mkString("\n")
  }


  protected def columnDDL(column: Column, nameWidth: Int, typeWidth: Int): String = {
    val nameFill = ' '.toString * (column.name.length - nameWidth)
    val columntype = column.dataType.getOrElse(throw new Exception(s"Cannot generate scalar column DDL from missing SQL data type for column ${column}"))
    val typeFill = ' '.toString * (columntype.length - typeWidth)
    if (column.key && column.required)
      s"  ${column.name}${nameFill} ${columntype}${typeFill} UNIQUE NOT NULL"
    else if(column.required)
      s"  ${column.name}${nameFill} ${columntype}${typeFill} NOT NULL"
    else
      s"  ${column.name}${nameFill} ${columntype}"
  }

  protected def foreignKeyDDL(column: Column): String = {
    val foreignTable = column.foreignTable.getOrElse(throw new Exception(s"Cannot generate foreign key DDL from missing foreign table for column ${column}"))
    val foreignNamespace = column.foreignNamespace.getOrElse(throw new Exception(s"Cannot generate foreign key DDL from missing foreign namespace for column ${column}"))
    val foreignKey = column.foreignKey.getOrElse(throw new Exception(s"Cannot generate foreign key DDL from missing foreign key for column ${column}"))
    s"  FOREIGN KEY(${column.name}) REFERENCES $foreignNamespace.$foreignTable($foreignKey)"
  }

  protected def keyColumnDDL(table: Table): String = {
    s"  PRIMARY KEY (${table.keyColumn})"
  }
}
