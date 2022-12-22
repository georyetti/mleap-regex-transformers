package georyetti.regex.mleap.spark.op

import georyetti.regex.mleap.model.RegexExtractModel
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.mleap.transformer.RegexExtract
import org.apache.spark.ml.bundle.SparkBundleContext

/**
 * Defines how to serialize/deserialize our RegexExtract Spark transformer to/from an MLEAP Bundle.
 * Extends [[OpNode]] and [[OpModel]].
 */
class RegexExtractOp extends OpNode[SparkBundleContext, RegexExtract, RegexExtractModel] {
  override val Model: OpModel[SparkBundleContext, RegexExtractModel] = new OpModel[SparkBundleContext, RegexExtractModel] {
    override val klazz: Class[RegexExtractModel] = classOf[RegexExtractModel]

    override def opName: String = "regex_extract"

    override def store(model: Model, obj: RegexExtractModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model
        .withValue("regexString", Value.string(obj.regexString))
        .withValue("idx", Value.int(obj.idx))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): RegexExtractModel = {
      RegexExtractModel(
        regexString = model.value("regexString").getString,
        idx = model.value("idx").getInt
      )
    }
  }
  override val klazz: Class[RegexExtract] = classOf[RegexExtract]

  override def name(node: RegexExtract): String = node.uid

  override def model(node: RegexExtract): RegexExtractModel = {
    node.model
  }

  override def load(node: Node, model: RegexExtractModel)
                   (implicit context: BundleContext[SparkBundleContext]): RegexExtract = {
    new RegexExtract(uid = node.name, model = model)
      .setInputCol(node.shape.standardInput.name)
      .setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: RegexExtract)(implicit context: BundleContext[SparkBundleContext]): NodeShape =
    NodeShape().withStandardIO(node.getInputCol, node.getOutputCol)
}
