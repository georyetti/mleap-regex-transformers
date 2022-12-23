package georyetti.regex.mleap.model

import ml.combust.mleap.core.types.{ScalarType, StructField}
import org.scalatest.funspec.AnyFunSpec

class RegexLikeModelTest extends AnyFunSpec {
  describe("RegexLikeModel") {

    val regexLikeModel = RegexLikeModel(regexString = "hat[^a]+")
    val regexLikeModelError = RegexLikeModel(regexString = "helloworld")
    val input = "hathatthattthatttt"

    it("test regex like with simple inputs") {

      val actual = regexLikeModel(input)

      assert(actual)
    }

    it("test regex like produces null if no match exists") {
      val actualFalse = regexLikeModelError(input)
      assert(!actualFalse)
    }

    it("test input schema is correct") {
      assert(regexLikeModel.inputSchema.fields ==
        Seq(StructField("input", ScalarType.String.nonNullable)))
    }
    it("test output schema is correct") {
      assert(regexLikeModel.outputSchema.fields ==
        Seq(StructField("output", ScalarType.Boolean.nonNullable)))
    }
  }
}
