package georyetti.regex.mleap.op

import georyetti.regex.mleap.model.RegexLikeModel
import georyetti.regex.mleap.transformer.RegexLike
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.runtime.MleapContext

/**
 * Defines how to serialize/deserialize our RegexLike model + transformer to/from an MLeap Bundle.
 * Extends [[MleapOp]] and [[OpModel]] for our transformer and model respectively.
 */
class RegexLikeOp extends MleapOp[RegexLike, RegexLikeModel] {
  override val Model: OpModel[MleapContext, RegexLikeModel] = new OpModel[MleapContext, RegexLikeModel] {
    override val klazz: Class[RegexLikeModel] = classOf[RegexLikeModel]

    override def opName: String = "regex_like"

    override def store(model: Model, obj: RegexLikeModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model
        .withValue("regexString", Value.string(obj.regexString))
    }
    override def load(model: Model)(implicit context: BundleContext[MleapContext]): RegexLikeModel = {
      RegexLikeModel(
        regexString = model.value("regexString").getString
      )
    }
  }

  override def model(node: RegexLike): RegexLikeModel = node.model

}

