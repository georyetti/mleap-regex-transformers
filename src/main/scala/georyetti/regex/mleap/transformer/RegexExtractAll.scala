package georyetti.regex.mleap.transformer

import georyetti.regex.mleap.model.RegexExtractAllModel
import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.frame.{SimpleTransformer, Transformer}
import ml.combust.mleap.runtime.function.UserDefinedFunction

/**
 * MLEAP transformer to execute a RegexExtractModel against a leap frame. Extends [[SimpleTransformer]].
 *
 * @param uid Unique name for the transformer.
 * @param shape Defines the input and output columns for the transformer.
 * @param model Model to execute, of class [[RegexExtractAllModel]].
 */
case class RegexExtractAll(override val uid: String = Transformer.uniqueName("regex_extract_all"),
                           override val shape: NodeShape,
                           override val model: RegexExtractAllModel) extends SimpleTransformer {

  override val exec: UserDefinedFunction = (value: String) => model(value)
}