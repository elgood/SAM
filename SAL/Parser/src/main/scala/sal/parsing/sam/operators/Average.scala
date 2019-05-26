package sal.parsing.sam.operators

import scala.collection.mutable.HashMap
import sal.parsing.sam.BaseParsing
import sal.parsing.sam.Constants

trait Average extends BaseParsing
{


  def ehAveOperator : Parser[EHAveExp] =
    ehAveKeyWord ~ "(" ~ identifier ~ "," ~ posInt ~ "," ~ posInt ~ ")" ^^
    {case ehave ~ lpar ~ id ~ c1 ~ n ~ c2 ~ k ~ rpar =>
      EHAveExp(id, n, k, memory)}

  def aveOperator : Parser[EHAveExp] =
    aveKeyWord ~ "(" ~ identifier ~ ")" ^^
    {case ehave ~ lpar ~ id ~ rpar =>
      val windowSize =  memory.getOrElse(Constants.WindowSize, "10000").toInt;
      val ehk = memory.getOrElse(Constants.EHK, "2").toInt; 
      EHAveExp(id, windowSize, ehk, memory)} |
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
  extends OperatorExp(field, memory)
{
  
  override def createOpString() = 
  {
    val lstream = memory.get(Constants.CurrentLStream).get
    
    // Creating an entry for the operator type of lstream so we can
    // look it up later.  For example, if we define blah to be a topk
    // feature, we can later figure out what blah.value(0) means in a
    // filter expression.
    memory += lstream + Constants.OperatorType -> Constants.EHSumKey
    
    val tupleType = memory.get(lstream + Constants.TupleTypeStr).get
    val numKeys = memory.get(lstream + Constants.NumKeysStr).get
    var keysString = ""
    for (i <- 0 until numKeys.toInt ) {
      keysString = keysString + 
        memory.get(lstream + Constants.KeyStr + i).get + ", " 
    }
    keysString = keysString.dropRight(2)
    
    var rString = "  identifier = \"" + lstream + "\";\n"
    rString += "  auto " + lstream + 
      " = std::make_shared<ExponentialHistogramAve<\n" +
      "    double, " + tupleType + ", " + field + ", " + keysString + ">>(\n" +
      "    " + N.toString + ", " + k.toString + 
      ", nodeId, featureMap, identifier);\n"
    rString += addRegisterStatements(lstream)
    rString
  }
  
}


