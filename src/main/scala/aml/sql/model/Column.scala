package aml.sql.model

case class Column(name: String, required: Boolean, propertyId: String, dataType: Option[String], foreignKey: Option[String], foreignNamespace: Option[String], foreignTable: Option[String])
