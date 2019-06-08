package sal.parsing.sam.statements

import scala.collection.mutable.HashMap

import sal.parsing.sam.BaseParsing
import sal.parsing.sam.Constants

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
    identifier ~> "=" ~> filterKeyWord ~ identifier ~ byKeyWord ~
      filterExpression ~ ";" ^^ 
    {
      case lstream ~ rstream ~ bkw ~ expression ~ colon =>
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

/**
 * The GreaterThanToken in a FILTER BY expression.  
 * SAM expects an expression as an infix list of tokens.  This is the 
 * token that represents >.   
 */
case class GreaterThanToken( memory: HashMap[String, String] )
extends LazyLogging
{
  /**
   * The toString method is called after a successful match of the parser.
   */
  override def toString  = {
    logger.info("greaterThan.toString")

    // The current left stream (variable being assigned) and its associated 
    // tuple type should have been placed in memory.  We need the tuple
    // type for a template parameter of the SAM class.
    val lstream = memory( Constants.CurrentLStream )
    val tupleType = memory( lstream + Constants.TupleTypeStr )
    
    // Create a unique variable name for the token.
    val greaterThanVar = "greaterThanOper" + GreaterThanToken.counter 

    // Incrementing the counter so the next > token gets a unique variable
    // name.
    GreaterThanToken.counter += 1

    // Creating the SAM code.
    var rString = "  auto " + greaterThanVar + " = std::make_shared<" +
      "GreaterThanOperator<" + tupleType + 
      ">>(featureMap);\n" +
      "\n"

    rString
  }
}

/**
 * The LessThanToken in a FILTER BY expression.  
 * SAM expects an expression as an infix list of tokens.  This is the 
 * token that represents <. 
 */
case class LessThanToken( memory: HashMap[String, String] )
extends LazyLogging
{
  /**
   * The toString method is called after a successful match of the parser.
   */
  override def toString  = {
    logger.info("lessThan.toString")

    // The current left stream (variable being assigned) and its associated 
    // tuple type should have been placed in memory.  We need the tuple
    // type for a template parameter of the SAM class.
    val lstream = memory( Constants.CurrentLStream )
    val tupleType = memory( lstream + Constants.TupleTypeStr )

    // Create a unique variable name for the token.
    val lessThanVar = "lessThanOper" + LessThanToken.counter 

    // Incrementing the counter so the next < token gets a unique variable
    // name.
    LessThanToken.counter += 1
    
    // Creating the SAM code.
    var rString = "  auto " + lessThanVar + " = std::make_shared<" +
      "LessThanOperator<" + tupleType + 
      ">>(featureMap);\n" +
      "\n"
    rString
  }
}

/**
 * The PlusToken in a FILTER BY expression.  
 * SAM expects an expression as an infix list of tokens.  This is the 
 * token that represents +. 
 */
case class PlusToken( memory: HashMap[String, String])
extends LazyLogging
{
  /**
   * The toString method is called after a successful match of the parser.
   */
  override def toString = {
    logger.info("PlusToken.toString") 

    // The current left stream (variable being assigned) and its associated 
    // tuple type should have been placed in memory.  We need the tuple
    // type for a template parameter of the SAM class.
    val lstream = memory( Constants.CurrentLStream )
    val tupleType = memory( lstream + Constants.TupleTypeStr )

    // Create a unique variable name for the token.
    val tokenVar = "plusToken" + PlusToken.counter
    
    // Incrementing the counter so the next plus token gets a unique variable
    // name.
    PlusToken.counter += 1

    // Creating the SAM code.
    val rString = "  auto " + tokenVar + 
                  " = std::make_shared<AddOperator<" + 
                  tupleType + ">>(featureMap)\n";
    rString
  }
}

 
/**
 * The MinusToken in a FILTER BY expression.  
 * SAM expects an expression as an infix list of tokens.  This is the 
 * token that represents -. 
 */
case class MinusToken(memory: HashMap[String,String] )
extends LazyLogging
{
  /**
   * The toString method is called after a successful match of the parser.
   */
  override def toString = {
    logger.info("MinusToken.toString")

    // The current left stream (variable being assigned) and its associated 
    // tuple type should have been placed in memory.  We need the tuple
    // type for a template parameter of the SAM class.
    val lstream = memory( Constants.CurrentLStream )
    val tupleType = memory( lstream + Constants.TupleTypeStr )
    
    // Create a unique variable name for the token.
    val tokenVar = "minusToken" + MinusToken.counter

    // Incrementing the counter so the next minus token gets a unique variable
    // name.
    MinusToken.counter += 1

    // Creating the SAM code.
    val rString = "  auto " + tokenVar + 
                  " = std::make_shared<MinusOperator<" +
                  tupleType + ">>(featureMap)\n";
    rString
  }
}


/**
 * The FloatToken in a FILTER BY expression.  
 * SAM expects an expression as an infix list of tokens.  This is the 
 * token that represents a float. 
 */
case class FloatToken(value : Float, memory: HashMap[String, String] )
extends LazyLogging
{
  override def toString = {

    logger.info("floatToken.toString")

    // The current left stream (variable being assigned) and its associated 
    // tuple type should have been placed in memory.  We need the tuple
    // type for a template parameter of the SAM class.
    val lstream = memory( Constants.CurrentLStream )
    val tupleType = memory( lstream + Constants.TupleTypeStr )

    // Create a unique variable name for the token.
    val tokenVar = "numberToken" + FloatToken.counter

    // Incrementing the counter so the next float token gets a unique variable
    // name.
    FloatToken.counter += 1

    // Creating the SAM code.
    var rString = "  auto " + tokenVar + " = std::make_shared<NumberToken<" + 
               tupleType + ">>(" +
               "featureMap, " + value + ");\n\n"
    rString
  }
}

///////////// Companion objects ///////////////////////////////////
// The following are companion objects for the classes representing
// tokens in a filter expression.  We create variables of the form
// <tokentype><x>
// where x is an integer.  To ensure a unique variable name, we start
// from zero and increment a static counter associated with the
// token type.
object GreaterThanToken { var counter = 0 }
object LessThanToken { var counter = 0 }
object PlusToken { var counter = 0 }
object MinusToken { var counter = 0 }
object FloatToken { var counter = 0 }



/**
 * servers = FILTER vertices GENERATE dest, average(edge.size) AS 
 *                                               (dest, asize)
 */
case class FilterStatement(lstream: String,
                            rstream: String,
                            expression: Any,
                            memory: HashMap[String, String])
extends Statement with LazyLogging
{
  override def toString = {
    logger.info("FilterStatement.toString")

    memory += Constants.CurrentLStream -> lstream
    transferKeyFields(lstream, rstream, memory)
    val tupleType = memory.get(lstream + Constants.TupleTypeStr).get
    
    var rString = ""

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



    /*rString += "  FilterExpression " + filterExpressionName + "(\"" + expression + "\");\n"
    rString += "  Filter* " + filterName + " = new Filter(" + filterExpressionName 
    rString += "," + keyFieldsVar + ", nodeId, " + imuxDataVar + ", \"" + lstream 
    rString += "\");\n"
    rString += "  consumer.registerConsumer(" + filterName + ");"*/


    // Some of the code generation took place without knowing what the 
    // tuple type was.  This replaces the placeholder with the actual
    // tuple type.
    rString
  }
}


