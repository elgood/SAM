package sal.parsing.sam.statements

import scala.collection.mutable.HashMap

import sal.parsing.sam.Constants

abstract class Statement {
 
  /**
   * This transfers the key fields from rstream to lstream.
   */
  def transferKeyFields(lstream: String,
                      rstream: String,
                      memory: HashMap[String, String]) : Unit =
  {
    val numKeys = memory.get(rstream + Constants.NumKeysStr).get
    memory += lstream + Constants.NumKeysStr -> numKeys
    for (i <- 0 to numKeys.toInt - 1) {
      val keykey = rstream + Constants.KeyStr + i.toString()
      val key = memory.get(keykey).get
      memory += lstream + Constants.KeyStr + i.toString() -> key           
    }
    val tupleType = memory.get(rstream + Constants.TupleTypeStr).get
    memory += lstream + Constants.TupleTypeStr -> tupleType
  }
}

abstract class ConnectionStatement extends Statement
