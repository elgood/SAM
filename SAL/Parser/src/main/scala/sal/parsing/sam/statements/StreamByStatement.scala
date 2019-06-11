package sal.parsing.sam.statements

import scala.collection.mutable.HashMap

import com.typesafe.scalalogging.LazyLogging

import sal.parsing.sam.BaseParsing
import sal.parsing.sam.Constants
import sal.parsing.sam.TupleTypes

trait StreamBy extends BaseParsing 
{
  def streamByStatement: Parser[StreamByStatement] = {
    (identifier ~ "=" ~ streamKeyWord ~ identifier ~ byKeyWord ~ 
      identifiers ~ ";") ^^
    { case lstream ~ equals ~ skw ~ rstream ~ bkw ~ features ~ colon =>
      StreamByStatement(lstream, rstream, features, memory)}
  }
}

object StreamByStatement { var counter = 0 }

/** 
 *  This looks for statements of the form
 *  lstream = STREAM rstream BY feature1, feature2, ...
 *  STREAM BY creates a logical partition while PARTITION BY creates
 *  an actual physical partitioning of the data.
 *  @param lstream The name of the partitioned stream being created.
 *  @param rstream The name of the single stream being read from.
 *  @param features A list of keys that are the basis for partitioning rstream
 *                  into lstream.
 *  @param memory A hashmap to keep track of things we need later.                
 */
case class StreamByStatement(lstream: String,
                             rstream: String,
                             features: List[String],
                             memory: HashMap[String, String])
  extends Statement with LazyLogging
{
  /**
   * The ForEach statement doesn't produce any code by itself, but instead
   * creates a mapping of the stream name to the associated key fields.
   * These key fields are then used as template parameters in later
   * operators.
   */
  override def toString = {
    logger.info("StreamByStatement.toString")
    // Get the tuple type of rstream
    val tupleType = memory.getOrElse(rstream + Constants.TupleType, 
                                     TupleTypes.Undefined)
                                     
    // Adding the tuple type for lstream to memory
    memory += lstream + Constants.TupleType -> tupleType
    
    // Adding the number of keys for lstream to memory
    memory += lstream + Constants.NumKeys -> features.length.toString()
    
    // Adding the keys for lstream to memory
    var i = 0
    for (feature <- features) {
      memory += lstream + Constants.KeyStr + i.toString ->
                feature
      i = i + 1
    } 
 
    // TODO: I don't really like this logic.  Probably a better way to do this.
    // How the code is generated currently, the first StreamByStatement has 
    // the variable name producer and afterwards we use the actual lstream name
    // assigned in SAL.  
    // We need to record the identifier associated with this stream.
    // It is just the identifier assigned in SAL.
    if (StreamByStatement.counter == 0) {
      memory += lstream + Constants.VarName -> Constants.Producer
    } else {
      memory += lstream + Constants.VarName -> lstream
    }

    // Keep track of how many StreamByStatements we have seen.
    StreamByStatement.counter += 1

    // Doesn't return anything, just sets values in memory. 
    "" 
  }
}


