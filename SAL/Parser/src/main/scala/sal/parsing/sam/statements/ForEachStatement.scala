package sal.parsing.sam.statements

import scala.collection.mutable.HashMap

import com.typesafe.scalalogging.LazyLogging

import sal.parsing.sam.BaseParsing
import sal.parsing.sam.Constants
import sal.parsing.sam.Util
import sal.parsing.sam.operators.Operator
import sal.parsing.sam.operators.OperatorExp

/**
 * Parser for ForEach statements, which are of the form:
 * lstream = FOREACH rstream GENERATE operator 
 */
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
 * Target class that is called when ForEach parser matches.
 * @param lstream The stream of features being created.
 * @param rstream The stream upon which the calculation is performed.
 * @param operator The operator being applied to the stream.
 * @param memory The hashmap with values defined previously.
 */
case class ForEachStatement(lstream: String,
                            rstream: String,
                            operator: OperatorExp,
                            memory: HashMap[String, String])
  extends Statement with Util with LazyLogging
{
  override def toString = {

    logger.info("ForEachStatement.toString")
    var rString = ""
    var count = 0
    
    memory += Constants.CurrentLStream -> lstream
    memory += Constants.CurrentRStream -> rstream
    
    // This gets the key/values from rstream and add them to lstream
    transferKeyFields(lstream, rstream, memory) 
    
    // We need to record the identifier associated with this stream.
    // It is just the identifier assigned in SAL.
    memory += lstream + Constants.VarName -> lstream

    rString += operator.toString
    
    rString  
  } 
}


