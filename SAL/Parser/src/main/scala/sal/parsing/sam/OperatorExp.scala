package sal.parsing.sam

import scala.collection.mutable.HashMap

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

/**
 * An expression of the form EHAve(field, N, k).  EH stands for exponential
 * histogram.
 * @param field This is a keyword of the high-level language
 *   indicating what field is being summed.
 * @param N The number of items in the window.
 * @param k Determines the size of the bins in the exponential histogram.
 */
case class EHAveExp(field: String, N: Int, k: Int,
                       memory: HashMap[String, String])
  extends OperatorExp(field, memory)
{
  
  override def createOpString() = 
  {
    val lstream = memory.get(Constants.CurrentLStream).get
    
    // Creating an entry for the operator type of lstream so we can
    // look it up later.  For example, if we define blah to be a topk
    // feature, we can later figure out what blah.value(0) means in a
    // filter expression.
    memory += lstream + Constants.OperatorType -> Constants.EHSumKey
    
    val tupleType = memory.get(lstream + Constants.TupleTypeStr).get
    val numKeys = memory.get(lstream + Constants.NumKeysStr).get
    var keysString = ""
    for (i <- 0 until numKeys.toInt ) {
      keysString = keysString + 
        memory.get(lstream + Constants.KeyStr + i).get + ", " 
    }
    keysString = keysString.dropRight(2)
    
    var rString = "  identifier = \"" + lstream + "\";\n"
    rString += "  auto " + lstream + 
      " = std::make_shared<ExponentialHistogramAve<\n" +
      "    double, " + tupleType + ", " + field + ", " + keysString + ">>(\n" +
      "    " + N.toString + ", " + k.toString + 
      ", nodeId, featureMap, identifier);\n"
    rString += addRegisterStatements(lstream)
    rString
  }
  
}


/**
 * An expression of the form EHSum(field, N, k).  EH stands for exponential
 * histogram.
 * @param field This is a keyword of the high-level language
 *   indicating what field is being summed.
 * @param N The number of items in the window
 * @param k Determines the size of the bins in the exponential histogram.
 */
case class EHSumExp(field: String, N: Int, k: Int,
                       memory: HashMap[String, String])
  extends OperatorExp(field, memory)
{
  
  override def createOpString() = 
  {
    val lstream = memory.get(Constants.CurrentLStream).get
    
    // Creating an entry for the operator type of lstream so we can
    // look it up later.  For example, if we define blah to be a topk
    // feature, we can later figure out what blah.value(0) means in a
    // filter expression.
    memory += lstream + Constants.OperatorType -> Constants.EHSumKey
    
    // Getting fields that are arguments of the template
    val tupleType = memory.get(lstream + Constants.TupleTypeStr).get
    val numKeys = memory.get(lstream + Constants.NumKeysStr).get
    var keysString = ""
    for (i <- 0 until numKeys.toInt ) {
      keysString = keysString + 
        memory.get(lstream + Constants.KeyStr + i).get + ", " 
    }
    keysString = keysString.dropRight(2)
    
    var rString = "  identifier = \"" + lstream + "\";\n"
    rString += "  auto " + lstream + " = std::make_shared<ExponenialHistogramSum<\n" +
      "    double, " + tupleType + ", " + field + ", " + keysString + ">>(" +
      N.toString + ", " + k.toString + ", nodeId, featureMap, identifier);\n"
    rString += addRegisterStatements(lstream)
    rString
  }
  
}

/**
 * An expression of the form EHVar(field, N, k).  EH stands for exponential
 * histogram.
 * @param lstream The stream var on the left side of the current line.
 * @param field This is a keyword of the high-level language
 *   indicating what field is being summed.
 * @param N The number of items in the window
 * @param k Determines the size of the bins in the exponential histogram.
 */
case class EHVarExp(field: String, N: Int, k: Int,
                       memory: HashMap[String, String])
  extends OperatorExp(field, memory)
{
  
  override def createOpString() = 
  {
    val lstream = memory.get(Constants.CurrentLStream).get
    
    // Creating an entry for the operator type of lstream so we can
    // look it up later.  For example, if we define blah to be a topk
    // feature, we can later figure out what blah.value(0) means in a
    // filter expression.
    memory += lstream + Constants.OperatorType -> Constants.EHVarKey
    
    // Getting fields that are arguments of the template
    val tupleType = memory.get(lstream + Constants.TupleTypeStr).get
    val numKeys = memory.get(lstream + Constants.NumKeysStr).get
    var keysString = ""
    for (i <- 0 until numKeys.toInt ) {
      keysString = keysString + 
        memory.get(lstream + Constants.KeyStr + i).get + ", " 
    }
    keysString = keysString.dropRight(2)
    
    var rString = "  identifier = \"" + lstream + "\";\n"
    rString = "  auto " + lstream + " = std::make_shard<ExponenialHistogramVariance<\n" +
      "    double, " + tupleType + ", " + field + ", " + keysString + ">>(" +
      N.toString + ", " + k.toString + ", nodeId, featureMap, identifier);\n"
    rString += addRegisterStatements(lstream)
    rString
  }
  
}

/**
 * An expression of the form simplesum(field, N, k).  
 * @param field This is a keyword of the high-level language
 *   indicating what field is being summed.
 * @param N The number of items in the window
 */
case class SimpleSumExp(field: String, N: Int,
                           memory: HashMap[String, String])
  extends OperatorExp(field, memory)
{

  override def createOpString() = 
  {
    val lstream = memory.get(Constants.CurrentLStream).get
    
    // Creating an entry for the operator type of lstream so we can
    // look it up later.  For example, if we define blah to be a topk
    // feature, we can later figure out what blah.value(0) means in a
    // filter expression.
    memory += lstream + Constants.OperatorType -> Constants.SimpleSumKey
    
    // Getting fields that are arguments of the template
    val tupleType = memory.get(lstream + Constants.TupleTypeStr).get
    val numKeys = memory.get(lstream + Constants.NumKeysStr).get
    var keysString = ""
    for (i <- 0 until numKeys.toInt ) {
      keysString = keysString + 
        memory.get(lstream + Constants.KeyStr + i).get + ", " 
    }
    keysString = keysString.dropRight(2)
    
    var rString = "  identifier = \"" + lstream + "\";\n"
    rString = "  auto " + lstream + " = std::make_shared<SimpleSum<\n" +
      "    double, " + tupleType + ", " + field + ", " + keysString + ">>(" +
      N.toString + "," + ", nodeId, featureMap, identifier);\n"
    rString += addRegisterStatements(lstream)
    rString
  }
  
}

/**
 * An expression of the form TopK(field, N, b, k).
 * \param field This is a keyword of the high-level language
 *   indicating what field is being aggregated (e.g. DestPort)
 * \param N The number of items in the window.
 * \param b The number of items in a sub-window.
 * \param k The number of top items to report.
 */
case class TopKExp(field: String, N: Int, b: Int, k: Int,
                      memory: HashMap[String, String]) 
  extends OperatorExp(field, memory)
{
  
  //TODO Needs to be fixed
  override def createOpString() = 
  {
    
    val keyFieldTemplateParameters = createKeyFieldsTemplateParameters
    
    val tupleType = getTupleType  
      
    val typeString = "TopK<size_t, " + tupleType + "," + field + "," +
                  keyFieldTemplateParameters + ">"
    val lstream = memory.get(Constants.CurrentLStream).get
    
    // Creating an entry for the operator type of lstream so we can
    // look it up later.  For example, if we define blah to be a topk
    // feature, we can later figure out what blah.value(0) means in a
    // filter expression.
    memory += lstream + Constants.OperatorType -> Constants.TopKKey
    
    // We need the input type later, too
    memory += lstream + Constants.InputType -> tupleType
    
    var rString = "  auto "  + lstream + 
                  " = new " + typeString + "(" +
                  N.toString + "," + b.toString + "," + k.toString + "," +
                  "nodeId, featureMap, \"" + lstream + "\");\n"

    rString
  }
  
}
