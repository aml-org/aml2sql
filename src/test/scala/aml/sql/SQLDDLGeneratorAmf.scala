package aml.sql



import amf.plugins.document.vocabularies.model.document.Dialect
import aml.sql.utils.AmfUtils
import org.scalatest.AsyncFunSuite

class SQLDDLGeneratorAmf extends AsyncFunSuite with AmfUtils {

  test("it should parse an AML dialect and generate an SQL DDL file") {
    loadDialect("file://src/test/resources/schemas.yaml") map { case dialect: Dialect =>
      val tablesParser = new DataBaseParser(dialect)
      val database = tablesParser.parse()
      val generated = SQLDDLGenerator.generate(database)
      val cs = platform.fs.syncFile("src/test/resources/schemas.sql").read()
      val target = cs.subSequence(0, cs.length()).toString
      assert(generated == target)
    }
  }

}