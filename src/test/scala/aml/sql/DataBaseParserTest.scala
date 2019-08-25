package aml.sql

import amf.plugins.document.vocabularies.model.document.Dialect
import org.scalatest.AsyncFunSuite

class DataBaseParserTest extends AsyncFunSuite with TestUtils {

  test("it should parse an AML dialect and generate a Database definition") {
    loadDialect("file://src/test/resources/schemas.yaml") map { case dialect: Dialect =>
      val tablesParser = new DataBaseParser(dialect)
      val database = tablesParser.parse()
      assert(database.tables.length == 2)
      assert(database.joinTables.length == 1)
    }
  }

}