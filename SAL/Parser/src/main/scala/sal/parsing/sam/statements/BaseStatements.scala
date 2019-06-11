package sal.parsing.sam.statements

import scala.collection.mutable.HashMap

import sal.parsing.sam.Constants

abstract class Statement {
 
  /**
   * This transfers the key fields from rstream to lstream.  This includes
   * 1) The key fields of the stream.  Which tuple fields are being used
   *    to logically partition the stream.
   * 2) The tuple type.
   * @param lstream The variable on the left (the one being defined).
   * @param rstream The variable on the right (the one being operated on).
   */
  def transferKeyFields(lstream: String,
                      rstream: String,
                      memory: HashMap[String, String]) : Unit =
  {
    // This transfers the key fields from the rstream to the lstream.
    val numKeys = memory(rstream + Constants.NumKeys)
    memory += lstream + Constants.NumKeys -> numKeys
    for (i <- 0 to numKeys.toInt - 1) {
      val keykey = rstream + Constants.KeyStr + i.toString()
      val key = memory.get(keykey).get
      memory += lstream + Constants.KeyStr + i.toString() -> key           
    }

    // This grabs the tuple type from the rstream and assigns it to the lstream.
    val tupleType = memory.get(rstream + Constants.TupleType).get
    memory += lstream + Constants.TupleType -> tupleType
  }

}

abstract class ConnectionStatement extends Statement
