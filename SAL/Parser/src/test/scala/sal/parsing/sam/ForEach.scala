import org.scalatest.FlatSpec
import sal.parsing.sam.Parsing
import sal.parsing.sam.Constants
import sal.parsing.sam.TupleTypes

class ForEachSpec extends FlatSpec with Parsing {
 
  //filtered = FILTER unfiltered BY 
  "A ForEach Statement" should "successfully match the statement" in {
    val tupleType = "EdgeType"
    memory += "VerticesBySrc" + Constants.NumKeys -> 1.toString
    memory += "VerticesBySrc" + Constants.KeyStr + 0 -> "DestIp"
    memory += "VerticesBySrc" + Constants.TupleType -> tupleType
    parseAll(forEachStatement,
      "feature1 = FOREACH VerticesBySrc GENERATE ave(SrcTotalBytes,10000,2);")
      match
    {
      case Success(matched,_) => 
      {
        val str = matched.toString
        assert(str.contains(tupleType))
        assert(str.contains(Constants.DefaultWindowSize + ", " +
                            Constants.DefaultEHK))
      }
      case Failure(msg,_) => assert(false)
      case Error(msg,_) => assert(false)
    }
  }

}
