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
  val EHK                   = "EHKVarKey"
  val DefaultEHK            = "2"

  // Some algorithms use a "basic window", which is basically a 
  // smaller window within the larger sliding window.
  val BasicWindowSize       = "BasicWindowSizeVarKey"
  val DefaultBasicWindowSize = "1000"

  // The size of the sliding window.
  val WindowSize            = "WindowSizeVarKey"
  val DefaultWindowSize     = "10000"

  val TopKK                 = "TopKKVarKey"
  
  val TopKKey               = "TopKKey"
  val EHSumKey              = "EHSumKey"
  val EHVarKey              = "EHVarKey"
  val SimpleSumKey          = "SimpleSumKey"
  val FilterKey             = "FilterKey"

  /************* end memory keys ********************/
  
  // Used to index into subfields of keys in memory
  val TupleType      = "TupleType"
  val TuplizerType   = "TuplizerType"
  val VarName        = "VarName"
  val NumKeys        = "NumKeys"
  val KeyStr         = "Key"
  val Subgraph       = "Subgraph"

  // Keeps track of the what the current lstream var is
  val CurrentLStream = "CurrentLStream"
  
  // Keeps track of the what the current rstream var is
  val CurrentRStream = "CurrentRStream"
  
  // Combined with lstream to form key so we can look up what
  // an operator type is for the given lstream.
  val OperatorType   = "OperatorType"
 
  // TODO: Make sure we need this 
  // The keys in the memory map to record the types of input tuples
  // from the connection statement.
  val ConnectionInputType = "ConnectionInputType"

  // Used to name the Hash types in HASH WITH statements
  val HashPrefix = "Hash"
  // Used to keep track of how many hash functions have been defined with
  // HASH WITH statements.
  val NumHashFunctions = "NumHashFunctionsKey"

  // Keeps track of the current infix variable name (for filter statements)
  val CurrentInfixVariable = "CurrentInfixVariable"

  // The variable name of the connection statement producer
  val Producer = "producer"

  // These are used to store template parameters for graphstore objects
  // for a particular stream
  val TimeField     = "TimeField"
  val DurationField = "DurationField"
  val SourceHash    = "SourceHash"
  val TargetHash    = "TargetHash"
  val SourceEq      = "SourceEq"
  val TargetEq      = "TargetEq"
}
