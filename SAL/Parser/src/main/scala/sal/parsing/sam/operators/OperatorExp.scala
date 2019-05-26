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
   * Creates a string of template parameters that can be inserted into 
   * the appropriate place within a template.
   * @param lstream This is the left stream var of the current line.
   * @param memory This holds things that were processed earlier that we 
   *               need now.
   */
  def createKeyFieldsTemplateParameters() : String =
      //lstream: String,
      //memory: HashMap[String, String]) : String = 
  {
    val lstream = memory.get(Constants.CurrentLStream).get
    
    var rstring = ""
    
    val numKeys = memory.get(lstream + Constants.NumKeysStr).get
    memory += lstream + Constants.NumKeysStr -> numKeys
    for (i <- 0 to numKeys.toInt - 1) {
      val keykey = lstream + Constants.KeyStr + i.toString()
      val key = memory.getOrElse(keykey, "Error with " + keykey)
      rstring = key + ","
    }   
    rstring = rstring.dropRight(1)
    
    rstring
  }
 
  /**
   * This regisers the newly created consumer (the operator) to
   * the active producer.
   * TODO: This assumes that the only producer is the original
   * one.  Add support for downstream producers.
   * It also registers the subscriber to the FeatureProducer 
   * (most operators are both consumers and feature producers).
   */ 
  def addRegisterStatements( varName: String ) : String = 
  {
    "  producer->registerConsumer(" + varName + ");\n" +
    "  if (subscriber != NULL) {\n" +
    "    " + varName + "->registerSubscriber(subscriber," +
    " identifier);\n" +
    "  }\n" +
    "\n"
  }
  
  /**
   * Gets the tuple type for the given operator.
   */
  def getTupleType() : String = 
    //lstream: String,
    //  memory: HashMap[String, String]) : String = 
  {
    val lstream = memory.get(Constants.CurrentLStream).get
    
    val rstring = memory.getOrElse(lstream + Constants.TupleTypeStr, "Error")
    rstring
  }
  
  override def toString = {
    val rString = createOpString()
    rString
  } 
}


 
