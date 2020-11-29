package sal.parsing.sam

/**
 * This lists the tuple types supported within SAM and the converter.  To add
 * a new tuple type, they must be listed here.
 * There are three pieces
 * 1) Tuple type
 * 2) Tuplizer type
 * 3) Connection statement
 *
 * @author elgood
 */
object TupleTypes {

  // Vast Tuple Type, Tuplizer, and connection key word
  val VastNetflow         = "VastNetflow"
  val VastNetflowTuplizer = "MakeVastNetflow"
  val VastConnect         = "VastStream"
  val VastNetflowNamespace = "sam::vast_netflow"

  val NetflowV5           = "NetflowV5"
  val NetflowV5Tuplizer   = "MakeNetflowV5"
  val NetflowV5Connect    = "NetflowV5Stream"
  val NetflowV5Namespace  = "sam::netflowv5"

  val Undefined           = "Undefined"
}
