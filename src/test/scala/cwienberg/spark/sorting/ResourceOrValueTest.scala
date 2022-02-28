package cwienberg.spark.sorting

import org.scalatest.funsuite.AnyFunSuite

class ResourceOrValueTest extends AnyFunSuite {

  test("isResource returns correct results") {
    assert(Resource("test").isResource)
    assert(!Value("test").isResource)
  }

  test("isValue returns correct results") {
    assert(!Resource("test").isValue)
    assert(Value("test").isValue)
  }

  test("getResource returns correct value or throws") {
    assert(Resource("test").getResource == "test")
    assertThrows[IllegalArgumentException] {
      Value("test").getResource
    }
  }

  test("getValue returns correct value or throws") {
    assertThrows[IllegalArgumentException] {
      Resource("test").getValue
    }
    assert(Value("test").getValue == "test")
  }

  test("Resources are less than Values") {
    assert(ResourceOrValue.ordering[String,String].compare(Resource("test"), Value("test")) < 0)
  }

  test("Values compared by actual value") {
    assert(ResourceOrValue.ordering[String,String].compare(Value("test1"), Value("test2")) < 0)
  }

  test("Comparing resources throws") {
    assertThrows[IllegalArgumentException] {
      ResourceOrValue.ordering[String,String].compare(Resource("test1"), Resource("test2"))
    }
  }
}
