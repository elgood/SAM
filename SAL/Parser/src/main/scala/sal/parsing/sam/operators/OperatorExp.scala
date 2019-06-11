package sal.parsing.sam.operators

import scala.collection.mutable.HashMap
import sal.parsing.sam.Constants

/**
 * @param lstream Is the stream on the left side e.g. top2
 * @param field Is the target field, e.g. DestPort
 */
abstract class OperatorExp(field: String,
                              memory: HashMap[String, String]) 
{
  
  //val lstream = memory.get(Constants.CurrentLStream).get
  
  /**
   * All the operators must define this abstract method.  It is how the string
   * is created that creates the c++ variable to represent the operation.
   */
  def createOpString() : String
                              

 
  /**
   * Gets the tuple type for the given operator.
   */
  def getTupleType() : String = 
    //lstream: String,
    //  memory: HashMap[String, String]) : String = 
  {
    val lstream = memory.get(Constants.CurrentLStream).get
    
    val rstring = memory.getOrElse(lstream + Constants.TupleType, "Error")
    rstring
  }
  
  override def toString = {
    val rString = createOpString()
    rString
  } 
}


 
