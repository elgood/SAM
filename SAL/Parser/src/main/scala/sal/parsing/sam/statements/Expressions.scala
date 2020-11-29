package sal.parsing.sam.statements

import sal.parsing.sam.BaseParsing
import scala.collection.mutable.HashMap
import sal.parsing.sam.Constants
import com.typesafe.scalalogging.LazyLogging

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


