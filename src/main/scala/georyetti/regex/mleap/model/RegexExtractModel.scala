package georyetti.regex.mleap.model

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{ScalarType, StructType}

/**
 * Core model class with implementation of regex extract shared between MLEAP and Spark.
 * Has no dependency on either MLEAP or Spark.
 *
 * Extracts the first string that matches the `regexString` expression and corresponds to the regex group index `idx`.
 *
 * @param regexString Regex pattern to match.
 * @param idx Optional non-negative group index to extract. Defaults to 1 if omitted
 */
case class RegexExtractModel(regexString: String, idx: Integer = 1) extends Model {

  // Group idx must be non-negative
  require(idx >= 0)

  def apply(input: String): String = {
    val re = regexString.r
    // TODO: Should be an nicer way to do this... is returning null good practice within Spark/MLEAP?
    val firstMatch = re.findFirstMatchIn(input)
    if (firstMatch.isDefined) firstMatch.get.group(idx) else null
  }

  override def inputSchema: StructType = StructType("input" -> ScalarType.String.nonNullable).get

  override def outputSchema: StructType = StructType("output" -> ScalarType.String.nonNullable).get
}


