import org.scalatest.FlatSpec
import sal.parsing.sam.Parsing
import sal.parsing.sam.Constants
import sal.parsing.sam.TupleTypes

class ForEachSpec extends FlatSpec with Parsing {

  "A ForEach Statement" should "successfully match the statement" in {
    memory += "VerticesBySrc" + Constants.NumKeysStr -> 1.toString
    memory += "VerticesBySrc" + Constants.KeyStr + 0 -> "DestIp"
    memory += "VerticesBySrc" + Constants.TupleTypeStr -> "Netflow"
    parseAll(forEachStatement,
      "feature1 = FOREACH VerticesBySrc GENERATE ave(SrcTotalBytes,10000,2);")
      match
    {
      case Success(matched,_) => println(matched); assert(true)
      case Failure(msg,_) => assert(false)
      case Error(msg,_) => assert(false)
    }
  }

}
