package sal.parsing.sam.statements

import scala.collection.mutable.HashMap
import sal.parsing.sam.BaseParsing
import sal.parsing.sam.Constants


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
extends Statement
{
  override def toString = {

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


