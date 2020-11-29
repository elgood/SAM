package sal.parsing.sam.operators

import scala.collection.mutable.HashMap
import sal.parsing.sam.BaseParsing
import sal.parsing.sam.Constants
import sal.parsing.sam.Util

/**
 * Parser for the ave operator.  There are different forms of how average
 * can be expressed in SAL, but they all use the same implementation,
 * an exponential histogram calculation.
 */
trait Average extends BaseParsing
{

  def ehAveOperator : Parser[EHAveExp] =
    // When the parameters are not specified
    ehAveKeyWord ~ "(" ~ identifier ~ ")" ^^
    {case ehave ~ lpar ~ id ~ rpar =>
      val windowSize =  memory.getOrElse(Constants.WindowSize, 
        Constants.DefaultWindowSize).toInt;
      val ehk = memory.getOrElse(Constants.EHK, 
        Constants.DefaultEHK).toInt; 
      EHAveExp(id, windowSize, ehk, memory)} |
    // When the parameters are specified.
    ehAveKeyWord ~ "(" ~ identifier ~ "," ~ posInt ~ "," ~ posInt ~ ")" ^^
    {case ehave ~ lpar ~ id ~ c1 ~ n ~ c2 ~ k ~ rpar =>
      EHAveExp(id, n, k, memory)} 

  def aveOperator : Parser[EHAveExp] =
    // When the parameters are not specified.
    aveKeyWord ~ "(" ~ identifier ~ ")" ^^
    {case ehave ~ lpar ~ id ~ rpar =>
      val windowSize =  memory.getOrElse(Constants.WindowSize, 
        Constants.DefaultWindowSize).toInt;
      val ehk = memory.getOrElse(Constants.EHK,
        Constants.DefaultEHK).toInt; 
      EHAveExp(id, windowSize, ehk, memory)} |
    // When the parameters are specified.
    aveKeyWord ~ "(" ~ identifier ~ "," ~ posInt ~ "," ~ posInt ~ ")" ^^
    {case ehave ~ lpar ~ id ~ c1 ~ n ~ c2 ~ k ~ rpar =>
      EHAveExp(id, n, k, memory)}

}

/**
 * An expression of the form EHAve(field, N, k).  EH stands for exponential
 * histogram.
 * @param field This is a keyword of the high-level language
 *   indicating what field is being summed.
 * @param N The number of items in the window.
 * @param k Determines the size of the bins in the exponential histogram.
 */
case class EHAveExp(field: String, N: Int, k: Int,
                       memory: HashMap[String, String])
  extends OperatorExp(field, memory) with Util
{
  
  override def createOpString() = 
  {
    // The characteristics of rstream has already been transferred 
    // to the lstream, so we can get what need from the features
    // indexed on the lstream in memory.
    val lstream = memory.get(Constants.CurrentLStream).get
    val rstream = memory.get(Constants.CurrentRStream).get
    
    // Creating an entry for the operator type of lstream so we can
    // look it up later.  For example, if we define blah to be a topk
    // feature, we can later figure out what blah.value(0) means in a
    // filter expression.
    memory += lstream + Constants.OperatorType -> Constants.EHSumKey
    
    // val tupleType = memory.get(lstream + Constants.TupleType).get
    val numKeys = memory.get(lstream + Constants.NumKeys).get
    var keysString = ""
    for (i <- 0 until numKeys.toInt ) {
      keysString = keysString + 
        memory.get(lstream + Constants.KeyStr + i).get + ", " 
    }
    keysString = keysString.dropRight(2)
    
    var rString = "  identifier = \"" + lstream + "\";\n"
    rString += "  auto " + lstream + 
      " = std::make_shared<ExponentialHistogramAve<\n" +
      "    double, EdgeType, " + field + ", " + keysString + ">>(\n" +
      "    " + N.toString + ", " + k.toString + 
      ", nodeId, featureMap, identifier);\n"
    rString += addRegisterStatements(lstream, rstream, memory)
    rString
  }
  
}


