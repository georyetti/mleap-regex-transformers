package org.apache.spark.ml.mleap.transformer

import georyetti.regex.mleap.model.RegexExtractAllModel
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{DataTypes, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
 * Spark transformer to execute a RegexExtractAllModel against a Spark dataframe. Extends [[Transformer]].
 *
 * @param uid Unique transformer name
 * @param model Model to execute, of class [[RegexExtractAllModel]].
 */
class RegexExtractAll(override val uid: String = Identifiable.randomUID("regex_extract_all"),
                      val model: RegexExtractAllModel)
  extends Transformer
    with HasInputCol
    with HasOutputCol {

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  @org.apache.spark.annotation.Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val modelUdf = udf {
      (input: String) => model(input)
    }

    dataset.withColumn($(outputCol), modelUdf(dataset($(inputCol)).cast(DataTypes.createArrayType(StringType))))
  }

  override def copy(extra: ParamMap): Transformer =
    copyValues(new RegexExtractAll(uid, model), extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    require(schema($(inputCol)).dataType.isInstanceOf[StringType],
      s"Input column must be of type StringType but got ${schema($(inputCol)).dataType}")
    val inputFields = schema.fields
    require(!inputFields.exists(_.name == $(outputCol)),
      s"Output column ${$(outputCol)} already exists.")

    StructType(schema.fields :+ StructField($(outputCol), DataTypes.createArrayType(StringType)))
  }
}
