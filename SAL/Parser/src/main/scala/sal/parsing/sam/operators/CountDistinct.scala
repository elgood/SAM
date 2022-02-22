package sal.parsing.sam.operators

import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.HashMap
import sal.parsing.sam.BaseParsing
import sal.parsing.sam.Constants
import sal.parsing.sam.Util

trait CountDistinct extends BaseParsing with LazyLogging
{
  def countDistinctOperator : Parser[CountDistinctExp] = 
    // When the parameters are not specified
    countDistinctKeyWord ~ "(" ~ identifier ~ ")" ^^
    {case cm ~ lpar ~ id ~ rpar =>
      val windowSize = memory.getOrElse(Constants.WindowSize,
        Constants.DefaultWindowSize).toInt;
      CountDistinctExp(id, windowSize, memory)} |
    // When the parameters are specified
    countDistinctKeyWord ~ "(" ~ identifier ~ "," ~ posInt ~ ")" ^^
    {case cm ~ lpar ~ id ~ c1 ~ n ~ rpar =>
      CountDistinctExp(id, n, memory)}
}

/**
 * An expression of the form CountDistinct(field, N).
 * @param field This is a keyword of the high-level language
 *   indicating what field where count distinct is being calculated.
 * @param N The number of items in the window
 */
case class CountDistinctExp(field: String, N: Int, 
                          memory: HashMap[String, String])
  extends OperatorExp(field, memory) with Util with LazyLogging
{
  override def createOpString() =
  {
    logger.info("CountDistinctExp.createOpString")
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
      " = std::make_shared<CountDistinct<\n" +
      "    size_t, EdgeType, " + field + ", " + keysString + ">>(" +
      N.toString + ", nodeId, featureMap, identifier);\n"
    rString += addRegisterStatements(lstream, rstream, memory)
    rString
  }
}


