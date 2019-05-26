package sal.parsing.sam.operators

import scala.collection.mutable.HashMap
import sal.parsing.sam.BaseParsing
import sal.parsing.sam.Constants

trait Variance extends BaseParsing
{

  def ehVarOperator : Parser[EHVarExp] =
    ehVarKeyWord ~ "(" ~ identifier ~ "," ~ posInt ~ "," ~ posInt ~ ")" ^^
    {case ehvar ~ lpar ~ id ~ c1 ~ n ~ c2 ~ k ~ rpar =>
      EHVarExp(id, n, k, memory)}

  def varOperator : Parser[EHVarExp] =
    varKeyWord ~ "(" ~ identifier ~ ")" ^^
    {case ehvar ~ lpar ~ id ~ rpar =>
      EHVarExp(id, 10000, 2, memory)}

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
  extends OperatorExp(field, memory)
{
  
  override def createOpString() = 
  {
    val lstream = memory.get(Constants.CurrentLStream).get
    
    // Creating an entry for the operator type of lstream so we can
    // look it up later.  For example, if we define blah to be a topk
    // feature, we can later figure out what blah.value(0) means in a
    // filter expression.
    memory += lstream + Constants.OperatorType -> Constants.EHVarKey
    
    // Getting fields that are arguments of the template
    val tupleType = memory.get(lstream + Constants.TupleTypeStr).get
    val numKeys = memory.get(lstream + Constants.NumKeysStr).get
    var keysString = ""
    for (i <- 0 until numKeys.toInt ) {
      keysString = keysString + 
        memory.get(lstream + Constants.KeyStr + i).get + ", " 
    }
    keysString = keysString.dropRight(2)
    
    var rString = "  identifier = \"" + lstream + "\";\n"
    rString = "  auto " + lstream + " = std::make_shard<ExponenialHistogramVariance<\n" +
      "    double, " + tupleType + ", " + field + ", " + keysString + ">>(" +
      N.toString + ", " + k.toString + ", nodeId, featureMap, identifier);\n"
    rString += addRegisterStatements(lstream)
    rString
  }
  
}


