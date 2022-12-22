package georyetti.regex.mleap.model

import ml.combust.mleap.core.types.{ScalarType, StructField}
import org.scalatest.funspec.AnyFunSpec

class RegexExtractModelTest extends AnyFunSpec {
  describe("RegexExtractModel") {

    val regexExtractModel = RegexExtractModel(regexString = "hat[^a]+", 0)
    val regexExtractModelError = RegexExtractModel(regexString = "helloworld", 0)
    val input = "hathatthattthatttt"

    it("test regex extract with simple inputs") {
      val expected= "hath"

      val actual = regexExtractModel(input)

      assert(expected == actual)
    }

    it("test regex extract produces null if no match exists") {
      val actualNull = regexExtractModelError(input)
      assert(Option(actualNull).isEmpty)
    }

    it("test input schema is correct") {
      assert(regexExtractModel.inputSchema.fields ==
        Seq(StructField("input", ScalarType.String.nonNullable)))
    }
    it("test output schema is correct") {
      assert(regexExtractModel.outputSchema.fields ==
        Seq(StructField("output", ScalarType.String.nonNullable)))
    }
  }
}
