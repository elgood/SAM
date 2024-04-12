package sal.parsing.sam.statements

import scala.collection.mutable.HashMap

import com.typesafe.scalalogging.LazyLogging

import sal.parsing.sam.BaseParsing
import sal.parsing.sam.TupleTypes
import sal.parsing.sam.Constants


/**
 * Parsing trait for handling a connection to a socket that streams
 * netflows with VHS22 format. 
 * https://www.kaggle.com/datasets/h2020simargl/vhs-22-network-traffic-dataset
 * 
 * Example SAL code:
 * edges = VHS22Stream(url, port);
 */
trait ConnectionVHS22 extends BaseParsing
{ 
  def connectionStatementVHS22: Parser[ConnectionStatementVHS22] =
    identifier ~ "=" ~ TupleTypes.VHS22Connect ~ "(" ~ url ~ "," ~
    port ~ ")" ~ ";" ^^
    { case identifier ~ eq ~ fkw ~ lpar ~ url ~ c ~ port ~ rpar ~ colon =>
      ConnectionStatementVHS22(identifier, url, port, memory)}
}

/**
 * The target when the parsing trait matches.  It doesn't actually
 * produce any code, just stores values in memory to be used at other
 * times in the code generation. 
 */
case class ConnectionStatementVHS22(stream: String,
                                   ip: String,
                                   port: Int,
                                   memory: HashMap[String, String])
extends ConnectionStatement with LazyLogging
{
  override def toString = {

    logger.info("ConnectionStatementVHS22.toString")

    // HashWith statements don't reference the stream name; it is assumed
    // to be the stream defined by the connection statement.  As such
    // we use the following memory mapping to keep track of that.
    memory += Constants.ConnectionInputType -> TupleTypes.VHS22Netflow
    memory += Constants.ConnectionInputTypeNamespace -> TupleTypes.VHS22NetflowNamespace

    // Also record the tuple type indexed by the name of the stream.
    // We'll need this later for any operators that are defined using this
    // stream.
    memory += stream + Constants.TupleType -> TupleTypes.VHS22Netflow

    // In addition the partition type needs the tuplizer, so record that
    // also.
    memory += stream + Constants.TuplizerType -> TupleTypes.VHS22NetflowTuplizer
    
    // We need the Tuplizer type when we don't know what the stream is.
    memory += Constants.ConnectionTuplizerType -> TupleTypes.VHS22NetflowTuplizer

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


