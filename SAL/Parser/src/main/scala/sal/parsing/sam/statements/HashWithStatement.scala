package sal.parsing.sam.statements

import scala.collection.mutable.HashMap
import com.typesafe.scalalogging.LazyLogging
import sal.parsing.sam.BaseParsing
import sal.parsing.sam.Constants

/**
 * Parsing trait to handle HashWith statements, e.g.
 *
 * HASH SourceIp WITH IpHashFunction;
 *
 * 
 */
trait HashWith extends BaseParsing
{
  def hashStatement =
    hashKeyWord ~ identifier ~ withKeyWord ~ hashFunction ~ ";" ^^
    { case hwk ~ identifier ~ wkw ~ hashFunction ~ scolon =>
      HashWithStatement(identifier, hashFunction, memory)}

  def hashStatements = hashStatement+
}
  


case class HashWithStatement(identifier: String, 
                                hashFunction: String,
                                memory: HashMap[String, String])
extends Statement with LazyLogging
{
  /**
   * When a match is made to method hashStatement, this target
   * is executed.  It creates a typedef c++ statement to create
   * a short name for the hash type that can be used as a template
   * parameter.  We start off with the name Hash0.  If there is more 
   * than one hash function, we just increment the counter,
   * e.g. Hash1. 
   */
  override def toString = {

    logger.info("HashWithStatement.toString")

    val myHashFunction : String = hashFunction match {
      case "IpHashFunction" => "TupleStringHashFunction"
      case _ => "Undefined" 
    }

    // Forming the name of the hash function and updating the count
    val hashId = memory.getOrElseUpdate(Constants.NumHashFunctions, "0")
    val hashName = Constants.HashPrefix + hashId
    memory(Constants.NumHashFunctions) =
      (memory(Constants.NumHashFunctions).toInt + 1).toString

    var output = "typedef " + myHashFunction + "<"
    output += memory(Constants.ConnectionInputType) + ","
    output += identifier + "> " + hashName + ";\n"
    output 
  }
}


