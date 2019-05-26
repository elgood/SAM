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
 
  /***************** Query stuff ********************/
  def queryStatements = queryStatement*
  def queryStatement = streamByStatement | forEachStatement | filterStatement
  
}


