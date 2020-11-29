package sal.parsing.sam

import scala.collection.mutable.HashMap

trait Util
{

  def flatten(ls: List[Any]): List[Any] = ls flatMap {
    case i: List[_] => flatten(i)
    case e => List(e)
  }

  /**
   * Creates a string of template parameters that can be inserted into 
   * the appropriate place within a c++ template.
   * @param lstream This is the left stream var of the current line.
   * @param memory This holds things that were processed earlier that we 
   *               need now.
   */
  def createKeyFieldsTemplateParameters(
    memory: HashMap[String, String]) : String =
  {
    val lstream = memory.get(Constants.CurrentLStream).get
    
    var rstring = ""
    
    val numKeys = memory.get(lstream + Constants.NumKeys).get
    memory += lstream + Constants.NumKeys -> numKeys
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
   * It also registers the subscriber to the FeatureProducer 
   * (most operators are both consumers and feature producers).
   */ 
  def addRegisterStatements( lstream: String ,
                             rstream: String,
                             memory: HashMap[String, String]) : String = 
  {
    //val producer = memory(rstream + Constants.VarName)
    //TODO: Below line breaks ml.sal example.  But the replacement line
    //probably breaks other sal examples.
    //"  " + producer + "->registerConsumer(" + lstream + ");\n" +
    "  producer->registerConsumer(" + lstream + ");\n" +
    "  if (subscriber != NULL) {\n" +
    "    " + lstream + "->registerSubscriber(subscriber," +
    " identifier);\n" +
    "  }\n" +
    "\n"
  }
 
}
