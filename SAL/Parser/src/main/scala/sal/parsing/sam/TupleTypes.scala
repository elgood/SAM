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
  val VastNetflowTuplizer = "VastNetflowTuplizer"
  val VastConnect         = "VastStream"



  val Undefined           = "Undefined"
}
