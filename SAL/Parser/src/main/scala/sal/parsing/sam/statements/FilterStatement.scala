package sal.parsing.sam.statements

import scala.collection.mutable.HashMap
import sal.parsing.sam.BaseParsing
import sal.parsing.sam.Constants
import sal.parsing.sam.Util
import scala.util.parsing.combinator.Parsers
import com.typesafe.scalalogging.LazyLogging

trait Filter extends BaseParsing with LazyLogging  {

  // As we create variables, we increase the counters to produce a unique
  // variable name.  These variables keep track of how many of each 
  // type of variable we've seen.
  protected var numberTokenCounter = 0
  protected var addOperCounter = 0
  protected var lessThanCounter = 0
  protected var greaterThanCounter = 0
  protected var minusCounter = 0
  protected var plusCounter = 0

  def filterStatement: Parser[FilterStatement] = {
    identifier ~ "=" ~ filterKeyWord ~ identifier ~ byKeyWord ~
      filterExpression ~ ";" ^^ 
    {
      case lstream ~ eq ~ fkw ~ rstream ~ bkw ~ expression ~ colon =>
      {
        logger.info("FilterStatement")
        FilterStatement(lstream, rstream, expression, memory)
      }
    }
  }

  def filterExpression = comparisonExpression

  def comparisonExpression = arithmeticExpression ~ comparator ~
                             arithmeticExpression 

  def arithmeticExpression = 
  token ~ arithmeticToken ~ token | token

  def comparator = lessThan | greaterThan

  
  def lessThan : Parser[LessThanToken] = "<" ^^ {
    case lt => LessThanToken(memory)
  }

  def greaterThan : Parser[GreaterThanToken] = ">" ^^ {
    case gt => GreaterThanToken(memory)
  }

 
  def token = floatToken | functionToken
  
  def floatToken : Parser[FloatToken] = """[+-]?([0-9]*[.])?[0-9]+""".r ^^ {
    case f => FloatToken(f.toFloat, memory)
  }

  def functionToken : Parser[FunctionToken] = {
    identifier ~ "." ~ identifier ~ "(" ~ int ~ ")" ^^
    {
      case id ~ dot ~ function ~ lparan ~ index ~ rparan =>
        logger.info("functionToken")
        FunctionToken(id, function, index, memory)
    }
  }
  
  def arithmeticToken = plusToken | minusToken
  
  def plusToken : Parser[PlusToken] = "+" ^^ { case p => PlusToken(memory) }
  def minusToken : Parser[MinusToken] = "-" ^^ { case m => MinusToken(memory) }

}
object FilterStatement { var counter = 0 }

/**
 * servers = FILTER vertices GENERATE dest, average(edge.size) AS 
 *                                               (dest, asize)
 */
case class FilterStatement(lstream: String,
                            rstream: String,
                            expression: Any,
                            memory: HashMap[String, String])
extends Statement with LazyLogging with Util
{
  override def toString = {
    logger.info("FilterStatement.toString")

    // We transfer the info from the rstream into the lstream so
    // we know the tuple type and so forth.
    memory += Constants.CurrentLStream -> lstream
    transferKeyFields(lstream, rstream, memory)
    val tupleType = memory(lstream + Constants.TupleType)

    memory += Constants.CurrentRStream -> rstream
    
    // SAM expects an infix list of filter expression tokens.  We
    // need to create the list that will hold the tokens in infix
    // order.
    var rString = ""
    var infixVar = "infixList" + FilterStatement.counter
    rString += "  std::list<std::shared_ptr<ExpressionToken<" +
      tupleType + ">>> " + infixVar + ";\n\n"
    memory += Constants.CurrentInfixVariable -> infixVar

    var expressionString = expression.toString

    // TODO The below block of code is a hack.  Calling toString on expression
    // creates extraneous text to be generated that must be removed.
    // There should be a way with case statements to generate the 
    // proper code without the extra stuff.  Need to figure out.
    val index = expressionString.indexOf(' ')
    expressionString =expressionString.substring(index, expressionString.length)
    expressionString = expressionString.replace(")~","")
    expressionString = expressionString.replace("~","")
    expressionString = expressionString.substring(0, expressionString.length -1)
    rString += expressionString

    // Gets the key fields needed for the template parameters.
    val keyFieldTemplateParameters = createKeyFieldsTemplateParameters(memory)

    val expressionVar = "filterExpression" + FilterStatement.counter

    rString += "  auto " + expressionVar + " = std::make_shared<" +
      "Expression<" + tupleType + ">>(" + infixVar + ");\n\n"
      

    rString += "  auto " + lstream + " = std::make_shared<Filter<" +
      tupleType + "," + keyFieldTemplateParameters + ">>(" +
      expressionVar + ",\n    nodeId, featureMap, \"" + lstream +
      "\", queueLength);\n\n"

    rString += "  " + "\n"

    // We need to record the identifier associated with this stream.
    // It is just the identifier assigned in SAL.
    memory += lstream + Constants.VarName -> lstream

    // Increment the counter for later filter statements so the variable
    // is unique.
    FilterStatement.counter += 1

    rString
  }
}


