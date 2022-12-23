package georyetti.regex.mleap.model

import ml.combust.mleap.core.types.{ListType, ScalarType, StructField}
import org.scalatest.funspec.AnyFunSpec

class RegexExtractAllModelTest extends AnyFunSpec {
  describe("RegexExtractAllModel") {

    val regexExtractAllModel = RegexExtractAllModel(regexString = "hat[^a]+", 0)
    val regexExtractAllModelError = RegexExtractAllModel(regexString = "helloworld", 0)
    val input = "hathatthattthatttt"

    it("test regex extract all with simple inputs") {
      val expected= Array("hath", "hattth")

      val actual = regexExtractAllModel(input)

      assert(expected sameElements actual)
    }

    it("test regex extract all produces empty array if no match exists") {
      val actualEmpty = regexExtractAllModelError(input)
      assert(actualEmpty.isEmpty)
    }

    it("test input schema is correct") {
      assert(regexExtractAllModel.inputSchema.fields ==
        Seq(StructField("input", ScalarType.String.nonNullable)))
    }
    it("test output schema is correct") {
      assert(regexExtractAllModel.outputSchema.fields ==
        Seq(StructField("output", ListType.String.nonNullable)))
    }
  }
}
