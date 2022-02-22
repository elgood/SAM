import org.scalatest.FlatSpec
import sal.parsing.sam.operators.Average
import sal.parsing.sam.Constants
import sal.parsing.sam.TupleTypes

class AverageSpec extends FlatSpec with Average {

  "An ehave operator" must "use default values in Constants when no" +
  " global defaults have been specified" in {
    memory.clear
    memory += Constants.CurrentLStream -> "features1"
    memory += Constants.CurrentRStream -> "VerticesBySource"
    memory += "features1" + Constants.TupleType -> "EdgeType"
    memory += "features1" + Constants.NumKeys -> 1.toString
    memory += "features1" + Constants.KeyStr + 0 -> "SourceIp"
    parseAll(ehAveOperator, "ehave(SrcTotalBytes)")
      match
    {
      case Success(matched,_) => 
         assert(matched.toString.contains(
           "std::make_shared<ExponentialHistogramAve<"))
         assert(matched.toString.contains(
           "double, EdgeType, SrcTotalBytes, SourceIp>"))
         assert(matched.toString.contains(Constants.DefaultWindowSize + 
           ", " + Constants.DefaultEHK))
      case Failure(msg,_) => assert(false)
      case Error(msg,_) => assert(false)
    }
  }

  "An ehave operator" must "use global defaults when specified but no " +
    "operator specific values have been defined." in
  {
    memory.clear
    memory += Constants.CurrentLStream -> "features1"
    memory += Constants.CurrentRStream -> "VerticesBySource"
    memory += "features1" + Constants.TupleType -> "EdgeType"
    memory += "features1" + Constants.NumKeys -> 1.toString
    memory += "features1" + Constants.KeyStr + 0 -> "SourceIp"
    memory += Constants.WindowSize -> "22"
    memory += Constants.EHK -> "5"
    parseAll(aveOperator, "ave(SrcTotalBytes)")
      match
    {
      case Success(matched,_) => 
         assert(matched.toString.contains(
           "std::make_shared<ExponentialHistogramAve<"))
         assert(matched.toString.contains(
           "double, EdgeType, SrcTotalBytes, SourceIp>"))
         assert(matched.toString.contains("22, 5"))
      case Failure(msg,_) => assert(false)
      case Error(msg,_) => assert(false)
    }
  }

  "An ave operator" must "use default values in Constants when no" +
  " global defaults have been specified and it should use the " +
  "ExponentialHistogramAve class" in {
    memory.clear
    memory += Constants.CurrentLStream -> "features1"
    memory += Constants.CurrentRStream -> "VerticesBySource"
    memory += "features1" + Constants.TupleType -> "EdgeType"
    memory += "features1" + Constants.NumKeys -> 1.toString
    memory += "features1" + Constants.KeyStr + 0 -> "SourceIp"
    parseAll(aveOperator, "ave(SrcTotalBytes)")
      match
    {
      case Success(matched,_) => 
         assert(matched.toString.contains(
           "std::make_shared<ExponentialHistogramAve<"))
         assert(matched.toString.contains(
           "double, EdgeType, SrcTotalBytes, SourceIp>"))
         assert(matched.toString.contains(Constants.DefaultWindowSize + 
           ", " + Constants.DefaultEHK))
      case Failure(msg,_) => assert(false)
      case Error(msg,_) => assert(false)
    }
  }

  "An ave operator" must "use global defaults when specified but no " +
    "operator specific values have been defined and it should use the " +
    "ExponentialHistogramAve class"  in
  {
    memory.clear
    memory += Constants.CurrentLStream -> "features1"
    memory += Constants.CurrentRStream -> "VerticesBySource"
    memory += "features1" + Constants.TupleType -> "EdgeType"
    memory += "features1" + Constants.NumKeys -> 1.toString
    memory += "features1" + Constants.KeyStr + 0 -> "SourceIp"
    memory += Constants.WindowSize -> "22"
    memory += Constants.EHK -> "5"
    parseAll(aveOperator, "ave(SrcTotalBytes)")
      match
    {
      case Success(matched,_) => 
         assert(matched.toString.contains(
           "std::make_shared<ExponentialHistogramAve<"))
         assert(matched.toString.contains(
           "double, EdgeType, SrcTotalBytes, SourceIp>"))
         assert(matched.toString.contains("22, 5"))
      case Failure(msg,_) => assert(false)
      case Error(msg,_) => assert(false)
    }
  }
}
