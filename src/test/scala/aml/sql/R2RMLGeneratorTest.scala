package aml.sql
import amf.plugins.document.vocabularies.model.document.Dialect
import org.scalatest.AsyncFunSuite

class R2RMLGeneratorTest extends AsyncFunSuite with TestUtils {

  test("it should parse an AML dialect and generate an SQL DDL file") {
    loadDialect("file://src/test/resources/schemas.yaml") map { case dialect: Dialect =>
      val tablesParser = new DataBaseParser(dialect)
      val database = tablesParser.parse()
      val generated = new R2RMLGenerator("", database).generate()
      val cs = platform.fs.syncFile("src/test/resources/schemas.r2rml").read()
      val target = cs.subSequence(0, cs.length()).toString
      assert(generated == target)
    }
  }

}