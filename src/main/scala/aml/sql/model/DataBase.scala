package aml.sql.model

case class DataBase(schemas: Seq[String], tables: Seq[Table], joinTables: Seq[JoinTable])
