package georyetti.regex.mleap.spark.op

import georyetti.regex.mleap.model.RegexReplaceModel
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.mleap.transformer.RegexReplace

/**
 * Defines how to serialize/deserialize our RegexReplace Spark transformer to/from an MLEAP Bundle.
 * Extends [[OpNode]] and [[OpModel]].
 */
class RegexReplaceOp extends OpNode[SparkBundleContext, RegexReplace, RegexReplaceModel] {
  override val Model: OpModel[SparkBundleContext, RegexReplaceModel] = new OpModel[SparkBundleContext, RegexReplaceModel] {
    override val klazz: Class[RegexReplaceModel] = classOf[RegexReplaceModel]

    override def opName: String = "regex_replace"

    override def store(model: Model, obj: RegexReplaceModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model
        .withValue("regexString", Value.string(obj.regexString))
        .withValue("replaceString", Value.string(obj.replaceString))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): RegexReplaceModel = {
      RegexReplaceModel(
        regexString = model.value("regexString").getString,
        replaceString = model.value("replaceString").getString
      )
    }
  }
  override val klazz: Class[RegexReplace] = classOf[RegexReplace]

  override def name(node: RegexReplace): String = node.uid

  override def model(node: RegexReplace): RegexReplaceModel = {
    node.model
  }

  override def load(node: Node, model: RegexReplaceModel)
                   (implicit context: BundleContext[SparkBundleContext]): RegexReplace = {
    new RegexReplace(uid = node.name, model = model)
      .setInputCol(node.shape.standardInput.name)
      .setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: RegexReplace)(implicit context: BundleContext[SparkBundleContext]): NodeShape =
    NodeShape().withStandardIO(node.getInputCol, node.getOutputCol)
}
