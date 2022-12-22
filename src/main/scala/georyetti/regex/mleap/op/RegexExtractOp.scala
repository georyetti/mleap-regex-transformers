package georyetti.regex.mleap.op

import georyetti.regex.mleap.model.RegexExtractModel
import georyetti.regex.mleap.transformer.RegexExtract
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.runtime.MleapContext

/**
 * Defines how to serialize/deserialize our RegexExtract model + transformer to/from an MLeap Bundle.
 * Extends [[MleapOp]] and [[OpModel]] for our transformer and model respectively.
 */
class RegexExtractOp extends MleapOp[RegexExtract, RegexExtractModel] {
  override val Model: OpModel[MleapContext, RegexExtractModel] = new OpModel[MleapContext, RegexExtractModel] {
    override val klazz: Class[RegexExtractModel] = classOf[RegexExtractModel]

    override def opName: String = "regex_extract"

    override def store(model: Model, obj: RegexExtractModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model
        .withValue("regexString", Value.string(obj.regexString))
        .withValue("idx", Value.int(obj.idx))
    }
    override def load(model: Model)(implicit context: BundleContext[MleapContext]): RegexExtractModel = {
      RegexExtractModel(
        regexString = model.value("regexString").getString,
        idx = model.value("idx").getInt
      )
    }
  }

  override def model(node: RegexExtract): RegexExtractModel = node.model

}

