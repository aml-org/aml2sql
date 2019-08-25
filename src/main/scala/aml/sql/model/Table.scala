package aml.sql.model

case class Table(namespace: String, name: String, classId:String, columns: Seq[Column], nodeMappingId: Option[String]) {
  val keyColumn: String = name + "_ID"
}
