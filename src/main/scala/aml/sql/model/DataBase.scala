package aml.sql.model

case class DataBase(tables: Seq[Table], joinTables: Seq[JoinTable])
