package georyetti.regex.mleap.spark.op

import georyetti.regex.mleap.model.RegexExtractAllModel
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.mleap.transformer.RegexExtractAll

/**
 * Defines how to serialize/deserialize our RegexExtractAll Spark transformer to/from an MLEAP Bundle.
 * Extends [[OpNode]] and [[OpModel]].
 */
class RegexExtractAllOp extends OpNode[SparkBundleContext, RegexExtractAll, RegexExtractAllModel] {
  override val Model: OpModel[SparkBundleContext, RegexExtractAllModel] = new OpModel[SparkBundleContext, RegexExtractAllModel] {
    override val klazz: Class[RegexExtractAllModel] = classOf[RegexExtractAllModel]

    override def opName: String = "regex_extract_all"

    override def store(model: Model, obj: RegexExtractAllModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model
        .withValue("regexString", Value.string(obj.regexString))
        .withValue("idx", Value.int(obj.idx))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): RegexExtractAllModel = {
      RegexExtractAllModel(
        regexString = model.value("regexString").getString,
        idx = model.value("idx").getInt
      )
    }
  }
  override val klazz: Class[RegexExtractAll] = classOf[RegexExtractAll]

  override def name(node: RegexExtractAll): String = node.uid

  override def model(node: RegexExtractAll): RegexExtractAllModel = {
    node.model
  }

  override def load(node: Node, model: RegexExtractAllModel)
                   (implicit context: BundleContext[SparkBundleContext]): RegexExtractAll = {
    new RegexExtractAll(uid = node.name, model = model)
      .setInputCol(node.shape.standardInput.name)
      .setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: RegexExtractAll)(implicit context: BundleContext[SparkBundleContext]): NodeShape =
    NodeShape().withStandardIO(node.getInputCol, node.getOutputCol)
}
