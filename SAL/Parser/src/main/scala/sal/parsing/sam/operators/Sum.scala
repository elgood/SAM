package sal.parsing.sam.operators

import scala.collection.mutable.HashMap
import sal.parsing.sam.BaseParsing
import sal.parsing.sam.Constants
import sal.parsing.sam.Util

trait Sum extends BaseParsing
{

  def ehSumOperator : Parser[EHSumExp] =
    // When the parameters are not specified
    ehSumKeyWord ~ "(" ~ identifier ~ ")" ^^
    {case ehsum ~ lpar ~ id ~ rpar =>
      val windowSize =  memory.getOrElse(Constants.WindowSize, 
        Constants.DefaultWindowSize).toInt;
      val ehk = memory.getOrElse(Constants.EHK, 
        Constants.DefaultEHK).toInt; 
      EHSumExp(id, windowSize, ehk, memory)} |
    // When the parameters are specified.
    ehSumKeyWord ~ "(" ~ identifier ~ "," ~ posInt ~ "," ~ posInt ~ ")" ^^
    {case ehsum ~ lpar ~ id ~ c1 ~ n ~ c2 ~ k ~ rpar =>
      EHSumExp(id, n, k, memory)}

  def sumOperator : Parser[EHSumExp] =
    // When the parameters are not specified.
    sumKeyWord ~ "(" ~ identifier ~ ")" ^^
    {case ehsum ~ lpar ~ id ~ rpar =>
      val windowSize =  memory.getOrElse(Constants.WindowSize, 
        Constants.DefaultWindowSize).toInt;
      val ehk = memory.getOrElse(Constants.EHK,
        Constants.DefaultEHK).toInt; 
      EHSumExp(id, windowSize, ehk, memory)} |
    // When the parameters are specified.
    sumKeyWord ~ "(" ~ identifier ~ "," ~ posInt ~ "," ~ posInt ~ ")" ^^
    {case ehsum ~ lpar ~ id ~ c1 ~ n ~ c2 ~ k ~ rpar =>
      EHSumExp(id, n, k, memory)}


  def simpleSumOperator : Parser[SimpleSumExp] =
    simpleSumKeyWord ~ "(" ~ identifier ~ ","  ~ posInt ~ ")" ^^
    {case ehvar ~ lpar ~ id ~ c1 ~ n ~ rpar =>
      SimpleSumExp(id, n, memory)}
}

/**
 * An expression of the form EHSum(field, N, k).  EH stands for exponential
 * histogram.
 * @param field This is a keyword of the high-level language
 *   indicating what field is being summed.
 * @param N The number of items in the window
 * @param k Determines the size of the bins in the exponential histogram.
 */
case class EHSumExp(field: String, N: Int, k: Int,
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
    memory += lstream + Constants.OperatorType -> Constants.EHSumKey
    
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
      " = std::make_shared<ExponentialHistogramSum<\n" +
      "    double, " + tupleType + ", " + field + ", " + keysString + ">>(" +
      N.toString + ", " + k.toString + ", nodeId, featureMap, identifier);\n"
    rString += addRegisterStatements(lstream, rstream, memory)
    rString
  }
  
}

/**
 * An expression of the form simplesum(field, N, k).  
 * @param field This is a keyword of the high-level language
 *   indicating what field is being summed.
 * @param N The number of items in the window
 */
case class SimpleSumExp(field: String, N: Int,
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
    memory += lstream + Constants.OperatorType -> Constants.SimpleSumKey
    
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
    rString = "  auto " + lstream + " = std::make_shared<SimpleSum<\n" +
      "    double, " + tupleType + ", " + field + ", " + keysString + ">>(" +
      N.toString + "," + ", nodeId, featureMap, identifier);\n"
    rString += addRegisterStatements(lstream, rstream, memory)
    rString
  }
  
}


