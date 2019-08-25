package aml.sql

import org.scalatest.FunSuite

class UtilsTest extends FunSuite with Utils {

  test("It should generate correct SQL identifiers") {
    assert(capitalize("this Is the Test") == "THIS_IS_THE_TEST")
    assert(capitalize("thisIsTheTest") == "THIS_IS_THE_TEST")
    assert(capitalize("thisIs_The_Test") == "THIS_IS_THE_TEST")
  }
}
