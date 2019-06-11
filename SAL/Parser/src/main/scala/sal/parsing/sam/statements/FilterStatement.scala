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

trait InfixUtil 
{
  /**
   * The tokens that are created need to be added to the an infix list.
   * This method creates the code that adds the tokenVar to the list.
   * @param tokenVar The name of the token variable.
   * @param memory The memory data structure holding the current infix
   *   variable name.
   * @return Returns a string with the code that adds the token var to
   *  to the infix list.
   */
  def infixPushBack( tokenVar : String, memory : HashMap[String, String] ) =
  {
    val infixListVariable = memory(Constants.CurrentInfixVariable)
    "  " + infixListVariable + ".push_back(" + tokenVar + ");\n"
  }
}


/**
 * The GreaterThanToken in a FILTER BY expression.  
 * SAM expects an expression as an infix list of tokens.  This is the 
 * token that represents >.   
 */
case class GreaterThanToken( memory: HashMap[String, String] )
extends LazyLogging with InfixUtil
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
    val tupleType = memory( lstream + Constants.TupleType )
    
    // Create a unique variable name for the token.
    val greaterThanVar = "greaterThanOper" + GreaterThanToken.counter 

    // Incrementing the counter so the next > token gets a unique variable
    // name.
    GreaterThanToken.counter += 1

    // Creating the SAM code.
    "  auto " + greaterThanVar + " = std::make_shared<" +
    "GreaterThanOperator<" + tupleType + 
    ">>(featureMap);\n" +
    infixPushBack( greaterThanVar, memory ) +
    "\n"
  }
}

/**
 * The LessThanToken in a FILTER BY expression.  
 * SAM expects an expression as an infix list of tokens.  This is the 
 * token that represents <. 
 */
case class LessThanToken( memory: HashMap[String, String] )
extends LazyLogging with InfixUtil
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
    val tupleType = memory( lstream + Constants.TupleType )

    // Create a unique variable name for the token.
    val lessThanVar = "lessThanOper" + LessThanToken.counter 

    // Incrementing the counter so the next < token gets a unique variable
    // name.
    LessThanToken.counter += 1
    
    "  auto " + lessThanVar + " = std::make_shared<" +
    "LessThanOperator<" + tupleType + 
    ">>(featureMap);\n" +
    infixPushBack( lessThanVar, memory ) +
    "\n"
  }
}

/**
 * The PlusToken in a FILTER BY expression.  
 * SAM expects an expression as an infix list of tokens.  This is the 
 * token that represents +. 
 */
case class PlusToken( memory: HashMap[String, String])
extends LazyLogging with InfixUtil
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
    val tupleType = memory( lstream + Constants.TupleType )

    // Create a unique variable name for the token.
    val tokenVar = "plusToken" + PlusToken.counter
    
    // Incrementing the counter so the next plus token gets a unique variable
    // name.
    PlusToken.counter += 1

    // Creating the SAM code.
    "  auto " + tokenVar + 
    " = std::make_shared<AddOperator<" + 
    tupleType + ">>(featureMap);\n" +
    infixPushBack( tokenVar, memory ) + "\n"
    
  }
}

 
/**
 * The MinusToken in a FILTER BY expression.  
 * SAM expects an expression as an infix list of tokens.  This is the 
 * token that represents -. 
 */
case class MinusToken(memory: HashMap[String,String] )
extends LazyLogging with InfixUtil
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
    val tupleType = memory( lstream + Constants.TupleType )
    
    // Create a unique variable name for the token.
    val tokenVar = "minusToken" + MinusToken.counter

    // Incrementing the counter so the next minus token gets a unique variable
    // name.
    MinusToken.counter += 1

    // Creating the SAM code.
    "  auto " + tokenVar + 
    " = std::make_shared<MinusOperator<" +
    tupleType + ">>(featureMap);\n" + 
    infixPushBack( tokenVar, memory ) + "\n"
  }
}


/**
 * The FloatToken in a FILTER BY expression.  
 * SAM expects an expression as an infix list of tokens.  This is the 
 * token that represents a float. 
 */
case class FloatToken(value : Float, memory: HashMap[String, String] )
extends LazyLogging with InfixUtil
{
  override def toString = {

    logger.info("floatToken.toString")

    // The current left stream (variable being assigned) and its associated 
    // tuple type should have been placed in memory.  We need the tuple
    // type for a template parameter of the SAM class.
    val lstream = memory( Constants.CurrentLStream )
    val tupleType = memory( lstream + Constants.TupleType )

    // Create a unique variable name for the token.
    val tokenVar = "numberToken" + FloatToken.counter

    // Incrementing the counter so the next float token gets a unique variable
    // name.
    FloatToken.counter += 1

    // Creating the SAM code.
    "  auto " + tokenVar + " = std::make_shared<NumberToken<" + 
    tupleType + ">>(" +
    "featureMap, " + value + ");\n" +
    infixPushBack( tokenVar, memory ) +
    "\n"
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


