package sal.parsing.sam.statements

import scala.collection.mutable.HashMap

import sal.parsing.sam.BaseParsing
import sal.parsing.sam.TupleTypes
import sal.parsing.sam.Constants


/**
 * Parsing trait for handling a connection to a socket that streams
 * netflows with Vast challenge format.
 * 
 * Example SAL code:
 * edges = VastStream(url, port);
 */
trait ConnectionVast extends BaseParsing
{ 
  def connectionStatementVast: Parser[ConnectionStatementVast] =
    identifier ~ "=" ~ TupleTypes.VastConnect ~ "(" ~ url ~ "," ~
    port ~ ")" ~ ";" ^^
    { case identifier ~ eq ~ fkw ~ lpar ~ url ~ c ~ port ~ rpar ~ colon =>
      ConnectionStatementVast(identifier, url, port, memory)}
}

/**
 * The target when the parsing trait matches.  It doesn't actually
 * produce any code, just stores values in memory to be used at other
 * times in the code generation. 
 */
case class ConnectionStatementVast(lstream: String,
                                   ip: String,
                                   port: Int,
                                   memory: HashMap[String, String])
extends ConnectionStatement
{
  override def toString = {

    // Record in the memory hashmap that we are expecting Vast netflow tuples
    // as the initial stream of data coming from the AbstractDataSource.
    memory += Constants.ConnectionInputType -> TupleTypes.VastNetflow
    memory += Constants.ConnectionTuplizerType -> TupleTypes.VastNetflowTuplizer

    // Also record the tuple type indexed by the name of the stream.
    // We'll need this later for any operators that are defined using this
    // stream.
    memory += lstream + Constants.TupleTypeStr -> TupleTypes.VastNetflow

    ""
  }  
}


