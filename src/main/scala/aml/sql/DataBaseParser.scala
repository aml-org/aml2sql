package aml.sql

import amf.plugins.document.vocabularies.model.document.Dialect
import amf.plugins.document.vocabularies.model.domain
import amf.plugins.document.vocabularies.model.domain.{NodeMapping, PropertyMapping}
import aml.sql.model.{Column, DataBase, JoinTable, Table}
import aml.sql.utils.Utils

class DataBaseParser(dialect: Dialect) extends Utils {

  case class DialectDeclaration(dialect: Dialect, nodeMapping: NodeMapping)

  def namespace(dialect: Dialect): String = capitalize(dialect.name().value())
  var joinTables: Seq[JoinTable] = Seq()

  def parse(): DataBase = {
    val ts = tables
    DataBase(Seq(namespace(dialect)),ts, joinTables)
  }

  protected lazy val tables: Seq[Table] = {
    val dialectDeclarations: Seq[DialectDeclaration] = dialect.declares.collect { case nm: NodeMapping => DialectDeclaration(dialect, nm) }

    // Let's first parse all the tables
    val acc: Seq[Table] = dialectDeclarations.map { dialectDeclaration =>
      parseTable(dialectDeclaration)
    }
    // let's now collect all the foreign tables
    val foreignAcc = dialect
      .references
      .collect({ case d: Dialect => d })
      .flatMap { d: Dialect =>
        d.declares.asInstanceOf[Seq[NodeMapping]].map { nodeMapping =>
          parseTable(DialectDeclaration(d, nodeMapping))
        }
      }

    // add both types of tables
    val totalAcc = acc ++ foreignAcc

    // table map
    val tableMap = totalAcc.foldLeft(Map[String, Table]()) { case (m, table) =>
        m.updated(table.classId, table)
    }

    // compute columns and join tables

    val tablesWithColumns = acc.map { table =>
      val dialectDeclaration = dialectDeclarations.find(_.nodeMapping.id == table.nodeMappingId.get).get
      val columns = columnsForTable(table, dialectDeclaration, tableMap)
      val keyColumn = Column(table.keyColumn, true, "http://aml.org/aml2sql/primaryKey", Some(PRIMARY_KEY_TYPE), None, None, None)
      table.copy(columns = Seq(keyColumn) ++ columns)
    }

    tablesWithColumns
  }

  protected def parseTable(dialectDeclaration: DialectDeclaration): Table = {
    val nodeMapping = dialectDeclaration.nodeMapping
    val dialect = dialectDeclaration.dialect
    val tableName = capitalize(nodeMapping.name.value())
    Table(namespace(dialect), tableName, nodeMapping.nodetypeMapping.value(), Nil, Some(nodeMapping.id))
  }

  protected def columnsForTable(table: Table, dialectDeclaration: DialectDeclaration, tableMap: Map[String,Table]): Seq[Column] = {
    val nodeMapping = dialectDeclaration.nodeMapping
    val dialect: Dialect = dialectDeclaration.dialect

    nodeMapping.propertiesMapping().map { propertyMapping =>
      if (propertyMapping.literalRange().option().isDefined) {
        val name = columnName(propertyMapping)
        val propertyId = propertyMapping.nodePropertyMapping().value()
        val sqlType = xsdToSql(propertyMapping.literalRange().value(), propertyMapping)
        val required = requiredColumn(propertyMapping)
        Some(Column(name, required, propertyId, Some(sqlType), None, None, None))
      } else if (propertyMapping.objectRange().size == 1 ) {
        computeJoinTable(dialect, table, propertyMapping, tableMap)
      } else if (propertyMapping.objectRange().size > 1) {
        throw new Exception("Properties with multiple ranges not supported yet")
      } else {
        throw new Exception("Cannot generate relational mapping for property mappings without literal or object range")
      }
    } collect { case Some(c) => c }
  }

  protected def computeJoinTable(dialect:Dialect, table: Table, propertyMapping: PropertyMapping, tableMap: Map[String, Table]): Option[Column] = {
    val targetNodeMappingId = propertyMapping.objectRange().head.value()

    val targetNodeMapping = try {
      dialect.findNodeMapping(targetNodeMappingId).getOrElse(throw new Exception(s"Cannot find node mapping with ID ${targetNodeMappingId} to compute JOIN table"))
    } catch {
      case e: Exception =>
        println(s"EXCEPTION !!! ${e} => for ${targetNodeMappingId}")
        return None
    }
    val targetClass = targetNodeMapping.nodetypeMapping.value()
    val targetTable = tableMap.getOrElse(targetClass, throw new Exception(s"Cannot define JOIN table for node mapping ${}"))
    val joinColumn = columnName(propertyMapping)

    if (propertyMapping.allowMultiple().option().isDefined) {
      // N:M mapping
      val joinTable = JoinTable(
        namespace(dialect),
        Some(joinColumn),
        table.name,
        table.keyColumn,
        Some(1),
        Some(propertyMapping.nodePropertyMapping().value()),
        targetTable.namespace,
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
      val column = Column(joinColumn, required, propertyMapping.nodePropertyMapping().value(), Some(PRIMARY_KEY_TYPE), Some(targetTable.keyColumn), Some(targetTable.namespace),  Some(targetTable.name))
      Some(column)
    }
  }

  protected def requiredColumn(propertyMapping: PropertyMapping): Boolean = propertyMapping.minCount().option().getOrElse(0) > 0
  protected def columnName(propertyMapping: PropertyMapping) = {
    capitalize(propertyMapping.name().value())
      .replace("30_", "THIRTY_")
      .replace("60_", "SIXTY_")
      .replace("90_", "NINETY_")
  }
}

