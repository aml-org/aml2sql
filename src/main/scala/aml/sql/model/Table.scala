package aml.sql.model

case class Table(namespace: String, name: String, classId:String, columns: Seq[Column], nodeMappingId: Option[String]) {
  def keyColumn: String = {
    val keyName = columns
      .find(_.key == true)
      .map(_.name)

    if (keyName.isEmpty) {
      throw new Exception(s"${namespace}.${name} => Missing primary key")
    }

    keyName.get
  }
}
