package sal.parsing.sam.statements

import scala.collection.mutable.HashMap
import sal.parsing.sam.BaseParsing
import sal.parsing.sam.Operator
import sal.parsing.sam.OperatorExp
import sal.parsing.sam.Constants
import sal.parsing.sam.Flatten

trait ForEach extends BaseParsing with Operator
{
  // e.g. feature1 = FOREACH VerticesBySrc GENERATE ehsum(DestPort,10000,2);
  def forEachStatement: Parser[ForEachStatement] = {
    identifier ~ "=" ~ forEachKeyWord ~ identifier ~ generateKeyWord ~
      operator ~ ";" ^^
    { case lstream ~ eq ~ fkw ~ rstream ~ gkw ~ operator ~ colon =>
      ForEachStatement(lstream, rstream, operator, memory)
    }  
  }
}
 

/**
 * ave = FOREACH vertices GENERATE dest, average(edge.size) AS 
 *                                               (dest, asize)
 */
case class ForEachStatement(lstream: String,
                            rstream: String,
                            operator: OperatorExp,
                            //listAgg: Product,
                            memory: HashMap[String, String])
  extends Statement with Flatten
{
  override def toString = {

    var rString = ""
    var count = 0
    
    memory += Constants.CurrentLStream -> lstream
    
    // This gets the keys from rstream and add them to lstream
    transferKeyFields(lstream, rstream, memory) 
    
    // Because we have operators = operator+, the "+" makes it so
    // that the result is an operator element followed by a list of 
    // operator elements.  Not sure the best way to do this
    //val operatorList = flatten(listAgg.productIterator.toList)
    //for (item <- operatorList) {
    //  rString += item + "\n"
      
    //}
    rString += operator.toString
    
    rString  
  } 
}


