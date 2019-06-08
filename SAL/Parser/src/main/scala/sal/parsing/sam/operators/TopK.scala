package sal.parsing.sam.operators

import scala.collection.mutable.HashMap
import sal.parsing.sam.BaseParsing
import sal.parsing.sam.Constants

trait TopK extends BaseParsing
{
  def topKOperator : Parser[TopKExp] =
    topKKeyWord ~ "(" ~ identifier ~ "," ~
      posInt ~ "," ~ posInt ~ "," ~ posInt ~ ")" ^^
      {case topk ~ lpar ~ id ~ c1 ~ n ~ c2 ~ b ~ c3 ~ k ~ rpar =>
        TopKExp(id, n, b, k, memory)}
}

/**
 * An expression of the form TopK(field, N, b, k).
 * \param field This is a keyword of the high-level language
 *   indicating what field is being aggregated (e.g. DestPort)
 * \param N The number of items in the window.
 * \param b The number of items in a sub-window.
 * \param k The number of top items to report.
 */
case class TopKExp(field: String, N: Int, b: Int, k: Int,
                      memory: HashMap[String, String]) 
  extends OperatorExp(field, memory)
{
  
  //TODO Needs to be fixed
  override def createOpString() = 
  {
    
    val keyFieldTemplateParameters = createKeyFieldsTemplateParameters
    
    val tupleType = getTupleType  
      
    val typeString = "TopK<size_t, " + tupleType + "," + field + "," +
                  keyFieldTemplateParameters + ">"
    val lstream = memory.get(Constants.CurrentLStream).get
    
    // Creating an entry for the operator type of lstream so we can
    // look it up later.  For example, if we define blah to be a topk
    // feature, we can later figure out what blah.value(0) means in a
    // filter expression.
    memory += lstream + Constants.OperatorType -> Constants.TopKKey
    
    // We need the input type later, too
    memory += lstream + Constants.InputType -> tupleType
    //println(memory)
    
    var rString = "  auto "  + lstream + 
                  " = new " + typeString + "(" +
                  N.toString + "," + b.toString + "," + k.toString + "," +
                  "nodeId, featureMap, \"" + lstream + "\");\n\n"

    rString
  }
} 
