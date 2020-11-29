package sal.parsing.sam.statements

import scala.collection.mutable.HashMap

import com.typesafe.scalalogging.LazyLogging

import sal.parsing.sam.BaseParsing
import sal.parsing.sam.Constants

/**
 * The parsing trait for handling partition statements i.e.
 * PARTITION <Stream> By <TupleElement1, ...>
 * PARTITION BY creates a physical partitioning of the data across the cluster.
 */ 
trait Partition extends BaseParsing
{ 
  /************** Partition statement ******************************/
  def partitionStatement = 
    partitionKeyWord ~ identifier ~ byKeyWord ~ identifiers ~ ";" ^^
    {case pkw ~ identifier ~ bkw ~ identifiers ~ scolon =>
      PartitionStatement(identifier, identifiers, memory)}
}

/**
 * The target of a successful Partition trait match.
 * Creates the SAM c++ code.  It generates typedef statements
 * in c++ for the PartitionType, the ReadCSV type, and the ProducerType.
 * The ProducerType is a BaseProducer, and can point to either one
 * of the PartitionType or CSV type so that we can use either
 * in the pipeline.   
 * @param stream The name of the stream to be partitioned.
 * @param fields The field names within the Tuple which are the
 *   basis for the partitioning.
 */
case class PartitionStatement(stream: String, 
                              fields: List[String],
                              memory: HashMap[String, String])
extends Statement with LazyLogging
{
  override def toString = {
    logger.info("PartitionStatement.toString")
    var output  = "    typedef ZeroMQPushPull<EdgeType, Tuplizer, " 

    // Figure out how many hash functions have been defined
    val numHashFunctions = memory(Constants.NumHashFunctions).toInt
    for (i <- 0 to numHashFunctions - 2) {
      output += Constants.HashPrefix + i.toString + ", "
    }
    output += Constants.HashPrefix + (numHashFunctions - 1).toString + "> "
    output += "PartitionType" + ";\n"
    output += "    typedef BaseProducer<EdgeType> ProducerType;\n" 
    output += "\n" 
    output += "    typedef ReadCSV<EdgeType, Tuplizer> ReadCSVType;\n" 
    output += "    typedef ReadSocket<EdgeType, Tuplizer> ReadSocketType;\n"
    output += "\n"

    val hashId = memory.getOrElseUpdate(Constants.NumHashFunctions, "0")
    // Reseting the number hash functions
    memory(Constants.NumHashFunctions) = "0"

    output
  }
}


