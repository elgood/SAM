import org.scalatest.FlatSpec
import sal.parsing.sam.operators.CountDistinct
import sal.parsing.sam.Constants
import sal.parsing.sam.TupleTypes

class CountDistinctSpec extends FlatSpec with CountDistinct {

  "A countdistinct operator" must "use default values in Constants when no" +
  " global defaults have been specified" in {
    memory.clear
    memory += Constants.CurrentLStream -> "features1"
    memory += Constants.CurrentRStream -> "VerticesBySource"
    memory += "features1" + Constants.TupleType -> "EdgeType"
    memory += "features1" + Constants.NumKeys -> 1.toString
    memory += "features1" + Constants.KeyStr + 0 -> "SourceIp"
    parseAll(countDistinctOperator, "countdistinct(SrcTotalBytes)")
      match
    {
      case Success(matched,_) => 
         assert(matched.toString.contains(
           "std::make_shared<CountDistinct<"))
         assert(matched.toString.contains(
           "size_t, EdgeType, SrcTotalBytes, SourceIp>"))
         assert(matched.toString.contains(Constants.DefaultWindowSize)) 
      case Failure(msg,_) => assert(false)
      case Error(msg,_) => assert(false)
    }
  }

  "An countdistnct operator" must "use global defaults when specified but no " +
    "operator specific values have been defined." in
  {
    memory.clear
    memory += Constants.CurrentLStream -> "features1"
    memory += Constants.CurrentRStream -> "VerticesBySource"
    memory += "features1" + Constants.TupleType -> "EdgeType"
    memory += "features1" + Constants.NumKeys -> 1.toString
    memory += "features1" + Constants.KeyStr + 0 -> "SourceIp"
    memory += Constants.WindowSize -> "22"
    parseAll(countDistinctOperator, "countdistinct(SrcTotalBytes)")
      match
    {
      case Success(matched,_) => 
         assert(matched.toString.contains(
           "std::make_shared<CountDistinct<"))
         assert(matched.toString.contains(
           "size_t, EdgeType, SrcTotalBytes, SourceIp>"))
         assert(matched.toString.contains("22"))
      case Failure(msg,_) => assert(false)
      case Error(msg,_) => assert(false)
    }
  }


}
