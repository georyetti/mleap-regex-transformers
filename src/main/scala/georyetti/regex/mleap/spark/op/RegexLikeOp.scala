package georyetti.regex.mleap.spark.op

import georyetti.regex.mleap.model.RegexLikeModel
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.mleap.transformer.RegexLike

/**
 * Defines how to serialize/deserialize our RegexLike Spark transformer to/from an MLEAP Bundle.
 * Extends [[OpNode]] and [[OpModel]].
 */
class RegexLikeOp extends OpNode[SparkBundleContext, RegexLike, RegexLikeModel] {
  override val Model: OpModel[SparkBundleContext, RegexLikeModel] = new OpModel[SparkBundleContext, RegexLikeModel] {
    override val klazz: Class[RegexLikeModel] = classOf[RegexLikeModel]

    override def opName: String = "regex_like"

    override def store(model: Model, obj: RegexLikeModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model
        .withValue("regexString", Value.string(obj.regexString))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): RegexLikeModel = {
      RegexLikeModel(
        regexString = model.value("regexString").getString,
      )
    }
  }
  override val klazz: Class[RegexLike] = classOf[RegexLike]

  override def name(node: RegexLike): String = node.uid

  override def model(node: RegexLike): RegexLikeModel = {
    node.model
  }

  override def load(node: Node, model: RegexLikeModel)
                   (implicit context: BundleContext[SparkBundleContext]): RegexLike = {
    new RegexLike(uid = node.name, model = model)
      .setInputCol(node.shape.standardInput.name)
      .setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: RegexLike)(implicit context: BundleContext[SparkBundleContext]): NodeShape =
    NodeShape().withStandardIO(node.getInputCol, node.getOutputCol)
}
