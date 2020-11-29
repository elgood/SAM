import org.scalatest.FlatSpec
import sal.parsing.sam.operators.Sum
import sal.parsing.sam.Constants
import sal.parsing.sam.TupleTypes

class SumSpec extends FlatSpec with Sum {

  "An ehsum operator" must "use default values in Constants when no" +
  " global defaults hsum been specified" in {
    memory.clear
    memory += Constants.CurrentLStream -> "features1"
    memory += Constants.CurrentRStream -> "VerticesBySource"
    memory += "features1" + Constants.TupleType -> "VastNetflow"
    memory += "features1" + Constants.NumKeys -> 1.toString
    memory += "features1" + Constants.KeyStr + 0 -> "SourceIp"
    parseAll(ehSumOperator, "ehsum(SrcTotalBytes)")
      match
    {
      case Success(matched,_) => 
         assert(matched.toString.contains(
           "std::make_shared<ExponentialHistogramSum<"))
         assert(matched.toString.contains(
           "double, VastNetflow, SrcTotalBytes, SourceIp>"))
         assert(matched.toString.contains(Constants.DefaultWindowSize + 
           ", " + Constants.DefaultEHK))
      case Failure(msg,_) => assert(false)
      case Error(msg,_) => assert(false)
    }
  }

  "An ehsum operator" must "use global defaults when specified but no " +
    "operator specific values hsum been defined." in
  {
    memory.clear
    memory += Constants.CurrentLStream -> "features1"
    memory += Constants.CurrentRStream -> "VerticesBySource"
    memory += "features1" + Constants.TupleType -> "VastNetflow"
    memory += "features1" + Constants.NumKeys -> 1.toString
    memory += "features1" + Constants.KeyStr + 0 -> "SourceIp"
    memory += Constants.WindowSize -> "22"
    memory += Constants.EHK -> "5"
    parseAll(sumOperator, "sum(SrcTotalBytes)")
      match
    {
      case Success(matched,_) => 
         assert(matched.toString.contains(
           "std::make_shared<ExponentialHistogramSum<"))
         assert(matched.toString.contains(
           "double, VastNetflow, SrcTotalBytes, SourceIp>"))
         assert(matched.toString.contains("22, 5"))
      case Failure(msg,_) => assert(false)
      case Error(msg,_) => assert(false)
    }
  }

  "An sum operator" must "use default values in Constants when no" +
  " global defaults hsum been specified and it should use the " +
  "ExponentialHistogramSum class" in {
    memory.clear
    memory += Constants.CurrentLStream -> "features1"
    memory += Constants.CurrentRStream -> "VerticesBySource"
    memory += "features1" + Constants.TupleType -> "VastNetflow"
    memory += "features1" + Constants.NumKeys -> 1.toString
    memory += "features1" + Constants.KeyStr + 0 -> "SourceIp"
    parseAll(sumOperator, "sum(SrcTotalBytes)")
      match
    {
      case Success(matched,_) => 
         assert(matched.toString.contains(
           "std::make_shared<ExponentialHistogramSum<"))
         assert(matched.toString.contains(
           "double, VastNetflow, SrcTotalBytes, SourceIp>"))
         assert(matched.toString.contains(Constants.DefaultWindowSize + 
           ", " + Constants.DefaultEHK))
      case Failure(msg,_) => assert(false)
      case Error(msg,_) => assert(false)
    }
  }

  "An sum operator" must "use global defaults when specified but no " +
    "operator specific values hsum been defined and it should use the " +
    "ExponentialHistogramSum class"  in
  {
    memory.clear
    memory += Constants.CurrentLStream -> "features1"
    memory += Constants.CurrentRStream -> "VerticesBySource"
    memory += "features1" + Constants.TupleType -> "VastNetflow"
    memory += "features1" + Constants.NumKeys -> 1.toString
    memory += "features1" + Constants.KeyStr + 0 -> "SourceIp"
    memory += Constants.WindowSize -> "22"
    memory += Constants.EHK -> "5"
    parseAll(sumOperator, "sum(SrcTotalBytes)")
      match
    {
      case Success(matched,_) => 
         assert(matched.toString.contains(
           "std::make_shared<ExponentialHistogramSum<"))
         assert(matched.toString.contains(
           "double, VastNetflow, SrcTotalBytes, SourceIp>"))
         assert(matched.toString.contains("22, 5"))
      case Failure(msg,_) => assert(false)
      case Error(msg,_) => assert(false)
    }
  }
}
