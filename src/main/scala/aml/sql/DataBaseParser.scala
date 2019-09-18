package aml.sql

import amf.core.model.domain.Linkable
import amf.core.vocabulary.Namespace
import amf.plugins.document.vocabularies.model.document.Dialect
import amf.plugins.document.vocabularies.model.domain.{NodeMapping, PropertyMapping}
import aml.sql.model.{Column, DataBase, JoinTable, Table}
import aml.sql.utils.Utils

import scala.collection.mutable

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
          val dialectDeclaration = DialectDeclaration(d, nodeMapping)
          val foreignTable = parseTable(dialectDeclaration)
          (dialectDeclaration, foreignTable)
        }
      }

    // add both types of tables
    val totalAcc = acc ++ foreignAcc.map(_._2)

    // table map
    val tableMap = totalAcc.foldLeft(mutable.Map[String, Table]()) { case (m, table) =>
        m.update(table.classId, table)
        m
    }

    // we compute columns and primary keys for foreign tables

    foreignAcc.toList.foreach { case (dialectDeclaration: DialectDeclaration, foreignTable: Table) =>
      val columns = ensurePrimaryKey(foreignTable, literalColumnsForTable(foreignTable, dialectDeclaration.nodeMapping, tableMap))
      val updatedTable = foreignTable.copy(columns = columns)
      tableMap.update(updatedTable.classId, updatedTable)
    }

    // compute columns, primary keys and join tables

    val tablesWithColumns = acc.toList.map { table =>
      val dialectDeclaration = dialectDeclarations.find(_.nodeMapping.id == table.nodeMappingId.get).get
      val columns = ensurePrimaryKey(table, literalColumnsForTable(table, dialectDeclaration.nodeMapping, tableMap))
      val updatedTable = table.copy(columns = columns)
      tableMap.update(updatedTable.classId, updatedTable)
      updatedTable
    }

    val tablesWithForeignColumns = tablesWithColumns.map { table =>
      val dialectDeclaration = dialectDeclarations.find(_.nodeMapping.id == table.nodeMappingId.get).get
      val columns = objectColumnsForTable(table, dialectDeclaration, tableMap)
      table.copy(columns = table.columns ++ columns)
    }

    tablesWithForeignColumns
  }

  protected def ensurePrimaryKey(table: Table, columns: Seq[Column]):Seq[Column] = {
    if (columns.exists(_.key == true)) {
      columns
    } else {
      Seq(Column(table.name + "_ID", key = true, required = true, "http://aml.org/aml2sql/primaryKey", Some(PRIMARY_KEY_TYPE), None, None, None)) ++ columns
    }
  }

  protected def parseTable(dialectDeclaration: DialectDeclaration): Table = {
    val nodeMapping = dialectDeclaration.nodeMapping
    val dialect = dialectDeclaration.dialect
    val tableName = capitalize(nodeMapping.name.value())
    Table(namespace(dialect), tableName, nodeMapping.nodetypeMapping.value(), Nil, Some(nodeMapping.id))
  }

  protected def isPrimaryKey(propertyMapping: PropertyMapping): Boolean = {
    propertyMapping.literalRange().option().contains((Namespace.Shapes + "guid").iri())
  }

  protected def literalColumnsForTable(table: Table, nodeMapping: NodeMapping, tableMap: mutable.Map[String,Table]): Seq[Column] = {
    val propertyMappings = nodeMapping.propertiesMapping()
    val extendedMappings = nodeMapping
      .extend
      .headOption
      .map({ baseNodeMapping =>
        baseNodeMapping.asInstanceOf[Linkable].linkTarget.get.asInstanceOf[NodeMapping].propertiesMapping()
      })
      .getOrElse(Nil)
      .filter { baseNodeMapping =>
        ! propertyMappings.exists(p => columnName(p) == columnName(baseNodeMapping) ) // remove duplicates
      }


    (propertyMappings ++ extendedMappings).map { propertyMapping =>
      if (propertyMapping.literalRange().option().isDefined) {
        val name = columnName(propertyMapping)
        val propertyId = propertyMapping.nodePropertyMapping().value()
        val sqlType = xsdToSql(propertyMapping.literalRange().value(), propertyMapping)
        val required = requiredColumn(propertyMapping)
        val primaryKey = isPrimaryKey(propertyMapping)
        if (primaryKey && !required) { throw new Exception(s"${table.namespace}.${table.name} => Primary keys must be required") }
        Some(Column(name, key = primaryKey, required, propertyId, Some(sqlType), None, None, None))
      } else {
        None
      }
    } collect { case Some(c) => c }
  }

  protected def objectColumnsForTable(table: Table, dialectDeclaration: DialectDeclaration, tableMap: mutable.Map[String,Table]): Seq[Column] = {
    val nodeMapping = dialectDeclaration.nodeMapping
    val dialect: Dialect = dialectDeclaration.dialect
    val propertyMappings = nodeMapping.propertiesMapping()
    val extendedMappings = nodeMapping
      .extend
      .headOption
      .map({ baseNodeMapping =>
        baseNodeMapping.asInstanceOf[Linkable].linkTarget.get.asInstanceOf[NodeMapping].propertiesMapping()
      })
      .getOrElse(Nil)
      .filter { baseNodeMapping =>
        ! propertyMappings.exists(p => columnName(p) == columnName(baseNodeMapping) ) // remove duplicates
      }


    (propertyMappings ++ extendedMappings).map { propertyMapping =>
      if (propertyMapping.objectRange().size == 1 ) {
        computeJoinTable(dialect, table, propertyMapping, tableMap)
      } else if (propertyMapping.objectRange().size > 1) {
        throw new Exception("Properties with multiple ranges not supported yet")
      } else {
        None
      }
    } collect { case Some(c) => c }
  }

  protected def computeJoinTable(dialect:Dialect, table: Table, propertyMapping: PropertyMapping, tableMap: mutable.Map[String, Table]): Option[Column] = {
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

    if (propertyMapping.allowMultiple().option().isDefined) {
      // N:M mapping
      val joinTable = JoinTable(
        leftNamespace = namespace(dialect),
        name = None,
        leftTable = table.name,
        leftColumn = table.keyColumn, // we don't use the join column here, we join by id
        leftCardinality = Some(1),
        leftProperty = Some(propertyMapping.nodePropertyMapping().value()),
        rightNamespace = targetTable.namespace,
        rightTable =  targetTable.name,
        rightColumn = targetTable.keyColumn,
        rightCardinality = propertyMapping.maximum().option().map(_.toInt),
        rightProperty = None
      )
      joinTables ++= Seq(joinTable)
      None
    } else {
      // 1:1 Mapping
      // in this case we use a join column
      var joinColumn = columnName(propertyMapping)
      joinColumn = if (joinColumn == "ID") s"${targetTable.name}_${joinColumn}" else joinColumn
      val required = requiredColumn(propertyMapping)
      val column = Column(joinColumn, key = false, required, propertyMapping.nodePropertyMapping().value(), Some(PRIMARY_KEY_TYPE), Some(targetTable.keyColumn), Some(targetTable.namespace),  Some(targetTable.name))
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

