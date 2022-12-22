package georyetti.regex.mleap.model

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{ScalarType, StructType}

/**
 * Core model class with implementation of regex replace shared between MLEAP and Spark.
 * Has no dependency on either MLEAP or Spark.
 *
 * Replaces all substrings in the input string that match `regexString` with `replaceString`.
 *
 * @param regexString Regex pattern to match.
 * @param replaceString String to replace all matches with.
 */
case class RegexReplaceModel(regexString: String, replaceString: String) extends Model {

  def apply(input: String): String = {
    val re = regexString.r
    re.replaceAllIn(input, replaceString)
  }

  override def inputSchema: StructType = StructType("input" -> ScalarType.String.nonNullable).get

  override def outputSchema: StructType = StructType("output" -> ScalarType.String.nonNullable).get
}