package sal.parsing.sam

import scala.collection.mutable.HashMap

import com.typesafe.scalalogging.LazyLogging

import sal.parsing.sam.statements.Statements
import sal.parsing.sam.subgraph.Subgraph
import sal.parsing.sam.preamble.Preamble

/**
 * @author elgood
 */
trait Parsing extends Statements with Preamble with Subgraph with LazyLogging 
{


  // TODO: I think this is too regimented.  Seems like there could be mulitiple
  // subgraphs defined and intermixing of query statements and subgraph 
  // definition. 
  def document = rep(preambleStatement) ~ connectionStatement ~ 
                 partitionStatement ~ hashStatements ~ rep(queryStatement) ~
                 rep(subgraph)  ^^
                {case preamble ~ connection ~ partition ~ hashStatements ~ 
                      query ~ subgraphs =>
                  logger.info("Parsing.document")
                  EntireQuery(connection, partition, hashStatements, 
                              query, subgraphs, memory)
                }
 
  def queryStatement = streamByStatement | forEachStatement | 
   filterStatement // | transformStatement 
  
}


