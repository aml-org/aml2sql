package aml.sql

import aml.sql.model.{Column, DataBase, JoinTable, Table}
import aml.sql.utils.Utils

object SQLDDLGenerator extends Utils {

  // Generates a self contained schema that replaces existing tables
  def generate(database: DataBase): String = {
    val tables = database.tables.map(t => tableDDL(t))
    val joinTables = database.joinTables.map(t => joinTableDDL(t))
    schemasDDL(database) + "\n\n" + (tables ++ joinTables).mkString("\n\n")
  }
  ////////////////////////

  // Functions used to generate the global schema
  // taking into account dependencies among tables
  def generateDeclarationSchemas(database: DataBase): String = schemasDDL(database)
  def generateDefinitionTables(database: DataBase): String = {
    database.tables.map(t => tableDefinitionDDL(t, global = true)).mkString("\n\n")
  }
  def generateTableConstraints(database: DataBase): String = {
    database.tables.map(t => tableConstraintDDL(t, global = true)).mkString("\n\n")
  }
  def generateDefinitionJoinTables(database: DataBase): String = {
    database.joinTables.map(t => joinTableDDL(t, global = true)).mkString("\n\n")
  }

  ////////////////////////

  protected def schemasDDL(database: DataBase): String = {
    database.schemas.map { schema =>
      s"CREATE SCHEMA IF NOT EXISTS ${schema};"
    } mkString("\n")
  }

  protected def joinTableDDL(joinTable: JoinTable, global: Boolean = false): String = {
    var leftKey = joinTable.leftKey
    var rightKey = joinTable.rightKey

    if (leftKey == rightKey) { // recursive table
      leftKey = leftKey + "_LEFT"
      rightKey = rightKey + "_RIGHT"
    }

    val lefPrefix = if (global) { "" } else { s"${joinTable.leftNamespace}." }
    val rightPrefix = if (global) { "" } else { s"${joinTable.rightNamespace}." }

    val start = s"CREATE TABLE ${lefPrefix}${joinTable.tableName} ("
    val leftColumn = s"  ${leftKey}  ${PRIMARY_KEY_TYPE} NOT NULL"
    val rightColumn = s"  ${rightKey} ${PRIMARY_KEY_TYPE} NOT NULL"
    val leftConstraint = s"  FOREIGN KEY(${leftKey}) REFERENCES ${lefPrefix}${joinTable.leftTable}(${joinTable.leftColumn})"
    val rightConstraint = s"  FOREIGN KEY(${rightKey}) REFERENCES ${rightPrefix}${joinTable.rightTable}(${joinTable.rightColumn})"
    val end = ");";

    val definition = Seq(leftColumn, rightColumn, leftConstraint, rightConstraint).mkString(",\n")
    Seq(start, definition, end).mkString("\n")
  }

  protected def tableDefinitionDDL(table: Table, global: Boolean = false): String = {
    val prefix = if (global) { "" } else { s"${table.namespace}." }
    val start = s"CREATE TABLE IF NOT EXISTS ${prefix}${table.name} ("
    val nameLength = table.columns.map(_.name.length).max
    val typeLength = table.columns.map(c => c.dataType.orElse(c.foreignKey)).collect{case Some(r: String) => r.length }.max

    var columns = table.columns.map { column =>
      columnDDL(column, nameLength + 4, typeLength + 4)
    }
    val declaration = columns.mkString(",\n")
    val end = ");"

    Seq(start, declaration, end).mkString("\n")
  }

  protected def tableConstraintDDL(table: Table, global: Boolean = false): String = {
    val prefix = if (global) { "" } else { s"${table.namespace}." }
    val start = s"ALTER TABLE IF EXISTS ${prefix}${table.name}"

    val constraints = Seq(keyColumnDDL(table)) ++ table.columns.filter(_.foreignKey.isDefined).map(c => foreignKeyDDL(c, global))

    val declaration = constraints.map("ADD " + _).mkString(",\n")
    val end = ";"

    if (constraints.nonEmpty)
      Seq(start, declaration, end).mkString("\n")
    else
      ""
  }

  protected def tableDDL(table: Table, global: Boolean = false): String = {
    val prefix = if (global) { "" } else { s"${table.namespace}." }
    val start = s"CREATE TABLE ${prefix}${table.name} ("
    val nameLength = table.columns.map(_.name.length).max
    val typeLength = table.columns.map(c => c.dataType.orElse(c.foreignKey)).collect{case Some(r: String) => r.length }.max

    var columns = table.columns.map { column =>
      columnDDL(column, nameLength + 4, typeLength + 4)
    }
    var constraints = Seq(keyColumnDDL(table)) ++ table.columns.filter(_.foreignKey.isDefined).map(c => foreignKeyDDL(c,global))

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

  protected def foreignKeyDDL(column: Column, global: Boolean = false): String = {
    val foreignTable = column.foreignTable.getOrElse(throw new Exception(s"Cannot generate foreign key DDL from missing foreign table for column ${column}"))
    val foreignNamespace = column.foreignNamespace.getOrElse(throw new Exception(s"Cannot generate foreign key DDL from missing foreign namespace for column ${column}"))
    val foreignKey = column.foreignKey.getOrElse(throw new Exception(s"Cannot generate foreign key DDL from missing foreign key for column ${column}"))
    val prefix = if (global) { "" } else { s"${foreignNamespace}." }
    s"  FOREIGN KEY(${column.name}) REFERENCES $prefix$foreignTable($foreignKey)"
  }

  protected def keyColumnDDL(table: Table): String = {
    s"  PRIMARY KEY (${table.keyColumn})"
  }
}
