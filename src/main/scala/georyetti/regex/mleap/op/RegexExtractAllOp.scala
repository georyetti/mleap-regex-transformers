package georyetti.regex.mleap.op

import georyetti.regex.mleap.model.RegexExtractAllModel
import georyetti.regex.mleap.transformer.RegexExtractAll
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.runtime.MleapContext

/**
 * Defines how to serialize/deserialize our RegexExtractAll model + transformer to/from an MLeap Bundle.
 * Extends [[MleapOp]] and [[OpModel]] for our transformer and model respectively.
 */
class RegexExtractAllOp extends MleapOp[RegexExtractAll, RegexExtractAllModel] {
  override val Model: OpModel[MleapContext, RegexExtractAllModel] = new OpModel[MleapContext, RegexExtractAllModel] {
    override val klazz: Class[RegexExtractAllModel] = classOf[RegexExtractAllModel]

    override def opName: String = "regex_extract_all"

    override def store(model: Model, obj: RegexExtractAllModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model
        .withValue("regexString", Value.string(obj.regexString))
        .withValue("idx", Value.int(obj.idx))
    }
    override def load(model: Model)(implicit context: BundleContext[MleapContext]): RegexExtractAllModel = {
      RegexExtractAllModel(
        regexString = model.value("regexString").getString,
        idx = model.value("idx").getInt
      )
    }
  }

  override def model(node: RegexExtractAll): RegexExtractAllModel = node.model

}

