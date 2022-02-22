package sal.parsing.sam.operators

import scala.collection.mutable.HashMap
import sal.parsing.sam.BaseParsing
import sal.parsing.sam.Constants
import sal.parsing.sam.Util

trait SelfSimilarity extends BaseParsing
{
  def selfSimilarityOperator : Parser[SelfSimilarityExp] = 
    // When the parameters are not specified
    selfSimilarityKeyWord ~ "(" ~ identifier ~ ")" ^^
    {case sim ~ lpar ~ id ~ rpar =>
      val windowSize = memory.getOrElse(Constants.WindowSize,
        Constants.DefaultWindowSize).toInt;
      SelfSimilarityExp(id, windowSize, memory)} |
    // When the parameters are specified
    selfSimilarityKeyWord ~ "(" ~ identifier ~ "," ~ posInt ~ ")" ^^
    {case sim ~ lpar ~ id ~ c1 ~ n ~ rpar =>
      SelfSimilarityExp(id, n, memory)}

}

/**
 * An expression of the form SelfSimilarity(field, N).
 * @param field This is a keyword of the high-level language
 *   indicating what field where self similarity is being computed.
 * @param N The number of items in the window
 */
case class SelfSimilarityExp(field: String, N: Int, 
                          memory: HashMap[String, String])
  extends OperatorExp(field, memory) with Util
{
  override def createOpString() =
  {
    val lstream = memory.get(Constants.CurrentLStream).get
    val rstream = memory.get(Constants.CurrentRStream).get

    // Getting fields that are arguments of the template`
    val tupleType = memory.get(lstream + Constants.TupleType).get
    val numKeys = memory.get(lstream + Constants.NumKeys).get
    var keysString = ""
    for (i <- 0 until numKeys.toInt ) {
      keysString = keysString +
        memory.get(lstream + Constants.KeyStr + i).get + ", "
    }
    keysString = keysString.dropRight(2)

    var rString = "  identifier = \"" + lstream + "\";\n"
    rString += "  auto " + lstream +
      " = std::make_shared<JaccardIndex<\n" +
      "    size_t, EdgeType, " + field + ", " + keysString + ">>(" +
      N.toString + ", nodeId, featureMap, identifier);\n"
    rString += addRegisterStatements(lstream, rstream, memory)
    rString

  }

}
