package sal.parsing.sam

import scala.collection.mutable.HashMap

import com.typesafe.scalalogging.LazyLogging

import sal.parsing.sam.statements.Statements
import sal.parsing.sam.preamble.Preamble

/**
 * @author elgood
 */
trait Parsing  extends Statements with Preamble with LazyLogging {

 
  def document = rep(preambleStatement) ~ connectionStatement ~ 
                 partitionStatement ~ hashStatements ~ rep(queryStatement) ^^
                {case preamble ~ connection ~ partition ~ hashStatements ~ 
                      query =>
                  logger.info("Parsing.document")
                  EntireQuery(connection, partition, hashStatements, 
                              query, memory)
                }
 
  def queryStatement = streamByStatement | forEachStatement | filterStatement
  
}


