package sal.parsing.sam.statements

import scala.collection.mutable.HashMap

import com.typesafe.scalalogging.LazyLogging

import sal.parsing.sam.BaseParsing
import sal.parsing.sam.TupleTypes
import sal.parsing.sam.Constants


/**
 * Parsing trait for handling a connection to a socket that streams
 * netflows with Netflow version 5.
 *
 * TODO: Should be able to template this.  Copied the code from Vast stuff.
 * 
 * Example SAL code:
 * edges = NetflowV5Stream(url, port);
 */
trait ConnectionNetflowV5 extends BaseParsing
{ 
  def connectionStatementNetflowV5: Parser[ConnectionStatementNetflowV5] =
    identifier ~ "=" ~ TupleTypes.NetflowV5Connect ~ "(" ~ url ~ "," ~
    port ~ ")" ~ ";" ^^
    { case identifier ~ eq ~ fkw ~ lpar ~ url ~ c ~ port ~ rpar ~ colon =>
      ConnectionStatementNetflowV5(identifier, url, port, memory)}
}

/**
 * The target when the parsing trait matches.  It doesn't actually
 * produce any code, just stores values in memory to be used at other
 * times in the code generation. 
 */
case class ConnectionStatementNetflowV5(stream: String,
                                   ip: String,
                                   port: Int,
                                   memory: HashMap[String, String])
extends ConnectionStatement with LazyLogging
{
  override def toString = {

    logger.info("ConnectionStatementNetflowV5.toString")

    // HashWith statements don't reference the stream name; it is assumed
    // to be the stream defined by the connection statement.  As such
    // we use the following memory mapping to keep track of that.
    memory += Constants.ConnectionInputType -> TupleTypes.NetflowV5
    memory += Constants.ConnectionInputTypeNamespace -> TupleTypes.NetflowV5Namespace

    // Also record the tuple type indexed by the name of the stream.
    // We'll need this later for any operators that are defined using this
    // stream.
    memory += stream + Constants.TupleType -> TupleTypes.NetflowV5

    // In addition the partition type needs the tuplizer, so record that
    // also.
    memory += stream + Constants.TuplizerType -> TupleTypes.NetflowV5Tuplizer
    
    // We need the Tuplizer type when we don't know what the stream is.
    memory += Constants.ConnectionTuplizerType -> TupleTypes.NetflowV5Tuplizer

    memory += stream + Constants.VarName -> Constants.Producer

    // These are fields needed by the GraphStore object
    memory += stream + Constants.TimeField     -> "TimeSeconds"
    memory += stream + Constants.DurationField -> "DurationSeconds"
    memory += stream + Constants.SourceHash    -> "StringHashFunction"
    memory += stream + Constants.TargetHash    -> "StringHashFunction"
    memory += stream + Constants.SourceEq      -> "StringEqualityFunction"
    memory += stream + Constants.TargetEq      -> "StringEqualityFunction"

    ""
  }  
}


