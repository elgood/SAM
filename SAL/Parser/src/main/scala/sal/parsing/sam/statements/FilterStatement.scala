package sal.parsing.sam.statements

import scala.collection.mutable.HashMap

import sal.parsing.sam.BaseParsing
import sal.parsing.sam.Constants


trait Filter extends BaseParsing {
  def filterStatement: Parser[FilterStatement] = {
    identifier ~ "=" ~ filterKeyWord ~ identifier ~ byKeyWord ~
      filterExpression ~ ";" ^^ 
    {
      case lstream ~ eq ~ fkw ~ rstream ~ bkw ~ expression ~ colon =>
      FilterStatement(lstream, rstream, expression, memory)

    }
  }

  //def filterExpression = token

  def filterExpression = comparisonExpression

  def comparisonExpression = arithmeticExpression ~ comparator ~
                             arithmeticExpression


  def arithmeticExpression = token ~ arithmeticOperator ~ token | token


             //int ^^ {_.toString} |
             //float ^^ { _.toString }
             //int ^^ {_.toString}|
             //functionExpression

  def comparator = "<" | ">"

  def token = floatToken | functionToken
  
  def floatToken : Parser[String] = """[+-]?([0-9]*[.])?[0-9]+""".r ^^ {
    def toString(arg : String)  = {
      val tokenVar = "numberToken" + uuid
      val inputType = memory.get(Constants.CurrentInputType).get
      var rString = "auto " + tokenVar + " = std::make_shared<NumberToken<" + 
                 inputType + ">>(featureMap, " + arg + ");"
      rString
    }
    toString(_)
  }
  
  def functionToken : Parser[FunctionToken] = {
    identifier ~ "." ~ identifier ~ "(" ~ int ~ ")" ^^
    {
      case id ~ dot ~ function ~ lparan ~ index ~ rparan =>
        FunctionToken(id, function, index, memory)
    }
  }
  
  def operatorToken = plusToken | minusToken
  
  def plusToken : Parser[String] = "+".r ^^ {
    def toString(arg : String) : String = {
      val tokenVar = "plusToken" + uuid
      val inputType = memory.get(Constants.CurrentInputType).get
      val rString = "  auto " + tokenVar + " = std::make_shared<AddOperator<" + 
                       inputType + ">>(featureMap)\n";
      rString
    }
    toString(_)
  }
  
  def minusToken : Parser[String] = "-".r ^^ {
    def toString(arg : String) : String = {
      val tokenVar = "plusToken" + uuid
      val inputType = memory.get(Constants.CurrentInputType).get
      val rString = "  auto " + tokenVar + " = std::make_shared<MinusOperator<" + 
                       inputType + ">>(featureMap)\n";
      rString
    }
    toString(_)
  }
 
}

/**
 * servers = FILTER vertices GENERATE dest, average(edge.size) AS 
 *                                               (dest, asize)
 */
case class FilterStatement(lstream: String,
                            rstream: String,
                            expression: Any,
                            memory: HashMap[String, String])
extends Statement
{
  override def toString = {
    
    var rString = ""
    rString += expression.toString()
    /*rString += "  FilterExpression " + filterExpressionName + "(\"" + expression + "\");\n"
    rString += "  Filter* " + filterName + " = new Filter(" + filterExpressionName 
    rString += "," + keyFieldsVar + ", nodeId, " + imuxDataVar + ", \"" + lstream 
    rString += "\");\n"
    rString += "  consumer.registerConsumer(" + filterName + ");"*/
    rString
  }
}


