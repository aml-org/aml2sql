package aml.sql

import amf.plugins.document.vocabularies.model.document.Dialect
import amf.plugins.document.vocabularies.model.domain
import amf.plugins.document.vocabularies.model.domain.{NodeMapping, PropertyMapping}
import aml.sql.model.{Column, DataBase, JoinTable, Table}

class DataBaseParser(dialect: Dialect) extends Utils {

  lazy val namespace: String = capitalize(dialect.name().value())
  var joinTables: Seq[JoinTable] = Seq()

  def parse(): DataBase = {
    val ts = tables
    DataBase(ts, joinTables)
  }

  protected lazy val tables: Seq[Table] = {
    val nodeMappings: Seq[domain.NodeMapping] = dialect.declares.collect { case nm: NodeMapping => nm }
    // Let's first parse all the tables
    val acc: Seq[Table] = nodeMappings.map { nodeMapping =>
      parseTable(nodeMapping)
    }

    // table map
    val tableMap = acc.foldLeft(Map[String, Table]()) { case (m, table) =>
        m.updated(table.classId, table)
    }

    // compute columns and join tables

    val tablesWithColumns = acc.map { table =>
      val nodeMapping = nodeMappings.find(_.id == table.nodeMappingId.get).get
      val columns = columnsForTable(table, nodeMapping, tableMap)
      val keyColumn = Column(table.keyColumn, true, "http://aml.org/aml2sql/primaryKey", Some(PRIMARY_KEY_TYPE), None, None)
      table.copy(columns = Seq(keyColumn) ++ columns)
    }

    tablesWithColumns
  }

  protected def parseTable(nodeMapping: NodeMapping): Table = {
    val tableName = capitalize(nodeMapping.name.value())
    Table(namespace, tableName, nodeMapping.nodetypeMapping.value(), Nil, Some(nodeMapping.id))
  }

  protected def columnsForTable(table: Table, nodeMapping: NodeMapping, tableMap: Map[String,Table]): Seq[Column] = {
    nodeMapping.propertiesMapping().map { propertyMapping =>
      if (propertyMapping.literalRange().option().isDefined) {
        val name = capitalize(propertyMapping.name().value())
        val propertyId = propertyMapping.nodePropertyMapping().value()
        val sqlType = xsdToSql(propertyMapping.literalRange().value(), propertyMapping)
        val required = requiredColumn(propertyMapping)
        Some(Column(name, required, propertyId, Some(sqlType), None, None))
      } else if (propertyMapping.objectRange().size == 1 ) {
        computeJoinTable(table, propertyMapping, tableMap)
      } else if (propertyMapping.objectRange().size > 1) {
        throw new Exception("Properties with multiple ranges not supported yet")
      } else {
        throw new Exception("Cannot generate relational mapping for property mappings without literal or object range")
      }
    } collect { case Some(c) => c }
  }

  protected def computeJoinTable(table: Table, propertyMapping: PropertyMapping, tableMap: Map[String, Table]): Option[Column] = {
    val targetNodeMappingId = propertyMapping.objectRange().head.value()
    val targetNode = dialect.declares.find(_.id == targetNodeMappingId).getOrElse(throw new Exception(s"Cannot find node mapping with ID ${targetNodeMappingId} to compute JOIN table")).asInstanceOf[NodeMapping]
    val targetClass = targetNode.nodetypeMapping.value()
    val targetTable = tableMap.getOrElse(targetClass, throw new Exception(s"Cannot define JOIN table for node mapping ${}"))
    val joinColumn = capitalize(propertyMapping.name().value())

    if (propertyMapping.allowMultiple().option().isDefined) {
      // N:M mapping
      val joinTable = JoinTable(
        namespace,
        Some(joinColumn),
        table.name,
        table.keyColumn,
        Some(1),
        Some(propertyMapping.nodePropertyMapping().value()),
        targetTable.name,
        targetTable.keyColumn,
        propertyMapping.maximum().option().map(_.toInt),
        None
      )
      joinTables ++= Seq(joinTable)
      None
    } else {
      // 1:1 Mapping
      val required = requiredColumn(propertyMapping)
      val column = Column(joinColumn, required, propertyMapping.nodePropertyMapping().value(), Some(PRIMARY_KEY_TYPE), Some(targetTable.keyColumn), Some(targetTable.namespace + "." + targetTable.name))
      Some(column)
    }
  }

  protected def requiredColumn(propertyMapping: PropertyMapping): Boolean = propertyMapping.minCount().option().getOrElse(0) > 0
}

