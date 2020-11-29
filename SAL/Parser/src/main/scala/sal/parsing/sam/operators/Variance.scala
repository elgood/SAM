package sal.parsing.sam.operators

import scala.collection.mutable.HashMap
import sal.parsing.sam.BaseParsing
import sal.parsing.sam.Constants
import sal.parsing.sam.Util

trait Variance extends BaseParsing
{

  def ehVarOperator : Parser[EHVarExp] =

    // When the parameters are not specified
    ehVarKeyWord ~ "(" ~ identifier ~ ")" ^^
    {case ehave ~ lpar ~ id ~ rpar =>
      val windowSize =  memory.getOrElse(Constants.WindowSize, 
        Constants.DefaultWindowSize).toInt;
      val ehk = memory.getOrElse(Constants.EHK, 
        Constants.DefaultEHK).toInt; 
      EHVarExp(id, windowSize, ehk, memory)} |
    // When the parameters are specified.
    ehVarKeyWord ~ "(" ~ identifier ~ "," ~ posInt ~ "," ~ posInt ~ ")" ^^
    {case ehvar ~ lpar ~ id ~ c1 ~ n ~ c2 ~ k ~ rpar =>
      EHVarExp(id, n, k, memory)}

  def varOperator : Parser[EHVarExp] =
    // When the parameters are not specified.
    varKeyWord ~ "(" ~ identifier ~ ")" ^^
    {case ehave ~ lpar ~ id ~ rpar =>
      val windowSize =  memory.getOrElse(Constants.WindowSize, 
        Constants.DefaultWindowSize).toInt;
      val ehk = memory.getOrElse(Constants.EHK,
        Constants.DefaultEHK).toInt; 
      EHVarExp(id, windowSize, ehk, memory)} |
    // When the parameters are specified.
    varKeyWord ~ "(" ~ identifier ~ "," ~ posInt ~ "," ~ posInt ~ ")" ^^
    {case ehave ~ lpar ~ id ~ c1 ~ n ~ c2 ~ k ~ rpar =>
      EHVarExp(id, n, k, memory)}

}

/**
 * An expression of the form EHVar(field, N, k).  EH stands for exponential
 * histogram.
 * @param lstream The stream var on the left side of the current line.
 * @param field This is a keyword of the high-level language
 *   indicating what field is being summed.
 * @param N The number of items in the window
 * @param k Determines the size of the bins in the exponential histogram.
 */
case class EHVarExp(field: String, N: Int, k: Int,
                       memory: HashMap[String, String])
  extends OperatorExp(field, memory) with Util
{
  
  override def createOpString() = 
  {
    val lstream = memory.get(Constants.CurrentLStream).get
    val rstream = memory.get(Constants.CurrentRStream).get
    
    // Creating an entry for the operator type of lstream so we can
    // look it up later.  For example, if we define blah to be a topk
    // feature, we can later figure out what blah.value(0) means in a
    // filter expression.
    memory += lstream + Constants.OperatorType -> Constants.EHVarKey
    
    // Getting fields that are arguments of the template
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
      " = std::make_shared<ExponentialHistogramVariance<\n" +
      "    double, EdgeType, " + field + ", " + keysString + ">>(" +
      N.toString + ", " + k.toString + ", nodeId, featureMap, identifier);\n"
    rString += addRegisterStatements(lstream, rstream, memory)
    rString
  }
  
}


