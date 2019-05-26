import org.scalatest.FlatSpec
import sal.parsing.sam.Parsing
import sal.parsing.sam.Constants
import sal.parsing.sam.TupleTypes

/**
 * Tests related to partition statements, e.g.
 * PARTITION <Stream> BY [Element1, Element2, ...]
 */
class PartitionSpec extends FlatSpec with Parsing with TestUtil {

  "The Partition key word" should "be case insensitive " in {
    shouldMatch(partitionKeyWord, "PARTITION")
    shouldMatch(partitionKeyWord, "Partition")
    shouldMatch(partitionKeyWord, "PartitioN")
    shouldMatch(partitionKeyWord, "partition")
    shouldntMatch(partitionKeyWord, "partition1")
  }

}
