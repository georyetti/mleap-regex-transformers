package georyetti.regex.mleap.op

import georyetti.regex.mleap.model.RegexReplaceModel
import georyetti.regex.mleap.transformer.RegexReplace
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.runtime.MleapContext

/**
 * Defines how to serialize/deserialize our RegexReplace model + transformer to/from an MLeap Bundle.
 * Extends [[MleapOp]] and [[OpModel]] for our transformer and model respectively.
 */
class RegexReplaceOp extends MleapOp[RegexReplace, RegexReplaceModel] {
  override val Model: OpModel[MleapContext, RegexReplaceModel] = new OpModel[MleapContext, RegexReplaceModel] {
    override val klazz: Class[RegexReplaceModel] = classOf[RegexReplaceModel]

    override def opName: String = "regex_replace"

    override def store(model: Model, obj: RegexReplaceModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model
        .withValue("regexString", Value.string(obj.regexString))
        .withValue("replaceString", Value.string(obj.replaceString))
    }
    override def load(model: Model)(implicit context: BundleContext[MleapContext]): RegexReplaceModel = {
      RegexReplaceModel(
        regexString = model.value("regexString").getString,
        replaceString = model.value("replaceString").getString
      )
    }
  }

  override def model(node: RegexReplace): RegexReplaceModel = node.model

}

