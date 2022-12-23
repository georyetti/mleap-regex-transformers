package georyetti.regex.mleap.model

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{ScalarType, StructType, ListType}

/**
 * Core model class with implementation of regex extract all shared between MLEAP and Spark.
 * Has no dependency on either MLEAP or Spark.
 *
 * Extracts all strings that match the `regexString` expression and correspond to the regex group index `idx`.
 *
 * @param regexString Regex pattern to match.
 * @param idx Group index to extract.
 */
case class RegexExtractAllModel(regexString: String, idx: Integer) extends Model {

  def apply(input: String): Array[String] = {
    val re = regexString.r
    re.findAllMatchIn(input).map(_.group(idx)).toArray
  }

  override def inputSchema: StructType = StructType("input" -> ScalarType.String.nonNullable).get

  override def outputSchema: StructType = StructType("output" -> ListType.String.nonNullable).get
}


