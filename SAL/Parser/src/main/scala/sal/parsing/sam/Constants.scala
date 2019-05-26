package sal.parsing.sam

/**
 * This has constants that are used in the generation of the c++
 * code.
 * @author elgood
 */
object Constants {
  
  /*********** memory keys **************************************/
  // The following are the keys used to store values in the 
  // memory hash structure.
  val QueueLength           = "QueueLengthVarKey" 
  val HighWaterMark         = "HighWaterMarkVarKey"

  // Exponential histograms have a parameter k, that determines
  // the tradeoff between space and approximation error.
  // 
  val EHK                   = "EHKVarKey"
  val BasicWindowSize       = "BasicWindowSizeVarKey"
  val WindowSize            = "WindowSizeVarKey"
  val TopKK                 = "TopKKVarKey"
  
  val TopKKey               = "TopKKey"
  val EHSumKey              = "EHSumKey"
  val EHVarKey              = "EHVarKey"
  val SimpleSumKey          = "SimpleSumKey"
  val FilterKey             = "FilterKey"

  /************* end memory keys ********************/
  
  // These are the names of variables used
  //val PortVar          = "port"
  //val IpVar            = "ip"
  //val ReadSocketVar    = "receiver"
  val QueueLengthVar   = "queueLength"
  //val HighWaterMarkVar = "hwm"
  
  // These are types used 
  val ReadSocketType = "ReadSocket"

  // Used to index into subfields of variables in memory
  val TupleTypeStr   = "TupleType"
  val NumKeysStr     = "NumKeys"
  val KeyStr         = "Key"

  // Keeps track of the what the current lstream var is
  val CurrentLStream = "CurrentLStream"
  
  // Combined with lstream to form key so we can look up what
  // an operator type is for the given lstream.
  val OperatorType   = "OperatorType"
  
  // Combined with lstream to form a key so we can look up what
  // the input type for an operator is.
  val InputType      = "InputType"
  
  val CurrentInputType = "CurrentInputType"
  
  // The keys in the memory map to record the types of input tuples and
  // associated tuplizer from the connection statement.
  val ConnectionInputType = "ConnectionInputType"
  val ConnectionTuplizerType = "ConnectionTuplizerType"

  // Used to name the Hash types in HASH WITH statements
  val HashPrefix = "Hash"
  // Used to keep track of how many hash functions have been defined with
  // HASH WITH statements.
  val NumHashFunctions = "NumHashFunctionsKey"

}
