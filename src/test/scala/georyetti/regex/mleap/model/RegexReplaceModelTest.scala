package georyetti.regex.mleap.model

import ml.combust.mleap.core.types.{ScalarType, StructField}
import org.scalatest.funspec.AnyFunSpec

class RegexReplaceModelTest extends AnyFunSpec {
  describe("RegexReplaceModel") {

    val regexReplaceModel = RegexReplaceModel(regexString = "[A-Z]", "")
    val RegexReplaceModelEmpty = RegexReplaceModel(regexString = "Hello World", "")
    val input = "Hello World"

    it("test regex replace with simple inputs") {
      val expected= "ello orld"

      val actual = regexReplaceModel(input)

      assert(expected == actual)
    }

    it("test regex replace produces empty string if perfect match") {
      val expected= ""
      val actual = RegexReplaceModelEmpty(input)
      assert(expected == actual)
    }

    it("test input schema is correct") {
      assert(regexReplaceModel.inputSchema.fields ==
        Seq(StructField("input", ScalarType.String.nonNullable)))
    }
    it("test output schema is correct") {
      assert(regexReplaceModel.outputSchema.fields ==
        Seq(StructField("output", ScalarType.String.nonNullable)))
    }
  }
}
