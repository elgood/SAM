package sal.parsing.sam

import scala.collection.mutable.HashMap
import sal.parsing.sam.statements.Statements
import sal.parsing.sam.preamble.Preamble

/**
 * @author elgood
 */
trait Parsing  extends Statements with Preamble {

 
  def document = preamble ~ connectionStatement ~ partitionStatement ~
                 hashStatements ~ queryStatements ^^
                {case preamble ~ connection ~ partition ~ hashStatements ~ 
                      query =>
                  EntireQuery(preamble, connection, partition, hashStatements, 
                              query, memory)
                }
 
  // Eventually plan to support other types of connections.  This should
  // be expanded to include other connection formats and tuples. 
  //def connectionStatement = connectionStatementVast

  // This parses the preamble statements and stores the values in a hashmap.
  //def preamble = preambleStatements ^^ { x => PreambleStatements(memory) }
  
  
  //def preambleStatements = preambleStatement*
  //def preambleStatement = queueLengthStatement | highWaterMarkStatement |
  //  ehKStatement | basicWindowSizeStatement | windowSizeStatement |
  //  topKKStatement 
                          
  /********************* Preamble stuff *******************/
  //def queueLengthStatement = 
  //  queueLengthKeyWord ~ "=" ~ posInt ~ ";" ^^
  //  {case kw ~ eq ~ length ~ semi =>
  //    memory += Constants.QueueLength -> length.toString}
  
  //def highWaterMarkStatement =
  //  highWaterMarkKeyWord ~ "=" ~ posInt ~ ";" ^^
  //  {case kw ~ eq ~ hwm ~ semi =>
  //    memory += Constants.HighWaterMark -> hwm.toString}
  
  //def ehKStatement = 
  //  ehKKeyWord ~ "=" ~ posInt ~ ";" ^^
  //  {case kkw ~ eq ~ k ~ semi => 
  //    memory += Constants.EHK -> k.toString}
  
  //def basicWindowSizeStatement = 
  //  basicWindowSizeKeyWord ~ "=" ~ posInt ~ ";" ^^
  //  {case bwskw ~ eq ~ size ~ semi =>
  //    memory += Constants.BasicWindowSize -> size.toString}
  
  //def windowSizeStatement = 
  //  windowSizeKeyWord ~ "=" ~ posInt ~ ";" ^^
  //  {case wskw ~ eq ~ size ~ semi =>
  //   memory += Constants.WindowSize -> size.toString}
  
  //def topKKStatement =
  //  topKKKeyWord ~ "=" ~ posInt ~ ";" ^^
  //  {case tkkkw ~ eq ~ size ~ semi =>
  //    memory += Constants.TopKK -> size.toString}
  // 
  
 
  /********* Connection Statement ******************************/ 
  // This matches statemens of the form:
  // edges = FlowStream(url, port)
  //def connectionStatementVast: Parser[ConnectionStatementVast] =
  //  identifier ~ "=" ~ TupleTypes.VastConnect ~ "(" ~ url ~ "," ~ 
  //  port ~ ")" ~ ";" ^^ 
  //  { case identifier ~ eq ~ fkw ~ lpar ~ url ~ c ~ port ~ rpar ~ colon => 
  //    ConnectionStatementVast(identifier, url, port, memory)}
 
  /************** Partition statement ******************************/
  //def partitionStatement = 
  //  partitionKeyWord ~ identifier ~ byKeyWord ~ identifiers ~ ";" ^^
  //  {case pkw ~ identifier ~ bkw ~ identifiers ~ scolon =>
  //    PartitionStatement(identifier, identifiers, memory)}

  /************** Hash statement ***********************************/
  //def hashStatement =
  //  hashKeyWord ~ identifier ~ withKeyWord ~ hashFunction ~ ";" ^^
  //  { case hwk ~ identifier ~ wkw ~ hashFunction ~ scolon =>
  //    HashWithStatement(identifier, hashFunction, memory)}

  //def hashStatements = hashStatement+
  
  /***************** Tokens **************************/
 
  /***************** Query stuff ********************/
  def queryStatements = queryStatement*
  def queryStatement = streamByStatement | forEachStatement | filterStatement
  
  //def streamByStatement: Parser[StreamByStatement] = {
  //  identifier ~ "=" ~ streamKeyWord ~ identifier ~ byKeyWord ~ identifiers ~ ";" ^^
  //  { case lstream ~ equals ~ skw ~ rstream ~ bkw ~ features ~ colon =>
  //    StreamByStatement(lstream, rstream, features, memory)}
  //}
  
 
  //def filterStatement: Parser[FilterStatement] = {
  //  identifier ~ "=" ~ filterKeyWord ~ identifier ~ byKeyWord ~ 
  //    filterExpression ~ ";" ^^  
  //  {
  //    case lstream ~ eq ~ fkw ~ rstream ~ bkw ~ expression ~ colon =>
  //    FilterStatement(lstream, rstream, expression, memory)    
  //    
  //  }
  //}
  
  
  
  //def filterExpression = token
  
  //def filterExpression = comparisonExpression
  
  //def comparisonExpression = arithmeticExpression ~ comparator ~ 
  //                           arithmeticExpression
  
                             
  //def arithmeticExpression = token ~ arithmeticOperator ~ token | token
  
  
             //int ^^ {_.toString} |
             //float ^^ { _.toString }
             //int ^^ {_.toString}|
             //functionExpression
  
  //def comparator = "<" | ">"
                             
  /** Operators **/
  //def operators = operator+
  /*def operator = topKOperator | 
                 ehSumOperator | sumOperator | 
                 ehVarOperator | varOperator |
                 ehAveOperator | aveOperator |
                 simpleSumOperator
  
  def topKOperator : Parser[TopKExp] =
    topKKeyWord ~ "(" ~ identifier ~ "," ~ 
      posInt ~ "," ~ posInt ~ "," ~ posInt ~ ")" ^^ 
      {case topk ~ lpar ~ id ~ c1 ~ n ~ c2 ~ b ~ c3 ~ k ~ rpar => 
        TopKExp(id, n, b, k, memory)}
  
  def ehSumOperator : Parser[EHSumExp] =
    ehSumKeyWord ~ "(" ~ identifier ~ "," ~ posInt ~ "," ~ posInt ~ ")" ^^
    {case ehsum ~ lpar ~ id ~ c1 ~ n ~ c2 ~ k ~ rpar =>
      EHSumExp(id, n, k, memory)}
  
  def sumOperator : Parser[EHSumExp] =
    sumKeyWord ~ "(" ~ identifier ~ ")" ^^
    {case ehsum ~ lpar ~ id  ~ rpar =>
      EHSumExp(id, 10000, 2, memory)}
  
  def ehAveOperator : Parser[EHAveExp] =
    ehAveKeyWord ~ "(" ~ identifier ~ "," ~ posInt ~ "," ~ posInt ~ ")" ^^
    {case ehave ~ lpar ~ id ~ c1 ~ n ~ c2 ~ k ~ rpar =>
      EHAveExp(id, n, k, memory)}
  
  def aveOperator : Parser[EHAveExp] =
    aveKeyWord ~ "(" ~ identifier ~ ")" ^^
    {case ehave ~ lpar ~ id ~ rpar =>
        

      EHAveExp(id, 10000, 2, memory)} |
    aveKeyWord ~ "(" ~ identifier ~ "," ~ posInt ~ "," ~ posInt ~ ")" ^^
    {case ehave ~ lpar ~ id ~ c1 ~ n ~ c2 ~ k ~ rpar =>
      EHAveExp(id, n, k, memory)} 

  
  def ehVarOperator : Parser[EHVarExp] =
    ehVarKeyWord ~ "(" ~ identifier ~ "," ~ posInt ~ "," ~ posInt ~ ")" ^^
    {case ehvar ~ lpar ~ id ~ c1 ~ n ~ c2 ~ k ~ rpar =>
      EHVarExp(id, n, k, memory)}
  
  def varOperator : Parser[EHVarExp] =
    varKeyWord ~ "(" ~ identifier ~ ")" ^^
    {case ehvar ~ lpar ~ id ~ rpar =>
      EHVarExp(id, 10000, 2, memory)}
  
  def simpleSumOperator : Parser[SimpleSumExp] =
    simpleSumKeyWord ~ "(" ~ identifier ~ ","  ~ posInt ~ ")" ^^
    {case ehvar ~ lpar ~ id ~ c1 ~ n ~ rpar =>
      SimpleSumExp(id, n, memory)}
  */  
  
}


