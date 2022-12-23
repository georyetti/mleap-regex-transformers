package georyetti.regex.mleap.model

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{ListType, ScalarType, StructType}

/**
 * Core model class with implementation of regex like shared between MLEAP and Spark.
 * Has no dependency on either MLEAP or Spark.
 *
 * Returns true if any matches are found for the `regexString` expression, or false otherwise.
 *
 * @param regexString Regex pattern to match.
 */
case class RegexLikeModel(regexString: String) extends Model {

  def apply(input: String): Boolean = {
    val re = regexString.r
    val firstMatch = re.findFirstMatchIn(input)
    if (firstMatch.isDefined) true else false
  }

  override def inputSchema: StructType = StructType("input" -> ScalarType.String.nonNullable).get

  override def outputSchema: StructType = StructType("output" -> ScalarType.Boolean.nonNullable).get
}


