package sal.parsing.sam

/**
 * @author elgood
 */
trait uuid {
  def uuid = java.util.UUID.randomUUID.toString
}
