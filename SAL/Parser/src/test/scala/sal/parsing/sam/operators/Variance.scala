import org.scalatest.FlatSpec
import sal.parsing.sam.operators.Variance
import sal.parsing.sam.Constants
import sal.parsing.sam.TupleTypes

class VarianceSpec extends FlatSpec with Variance {

  "An ehvar operator" must "use default values in Constants when no" +
  " global defaults hvar been specified" in {
    memory.clear
    memory += Constants.CurrentLStream -> "features1"
    memory += Constants.CurrentRStream -> "VerticesBySource"
    memory += "features1" + Constants.TupleType -> "EdgeType"
    memory += "features1" + Constants.NumKeys -> 1.toString
    memory += "features1" + Constants.KeyStr + 0 -> "SourceIp"
    parseAll(ehVarOperator, "ehvar(SrcTotalBytes)")
      match
    {
      case Success(matched,_) => 
         assert(matched.toString.contains(
           "std::make_shared<ExponentialHistogramVariance<"))
         assert(matched.toString.contains(
           "double, EdgeType, SrcTotalBytes, SourceIp>"))
         assert(matched.toString.contains(Constants.DefaultWindowSize + 
           ", " + Constants.DefaultEHK))
      case Failure(msg,_) => assert(false)
      case Error(msg,_) => assert(false)
    }
  }

  "An ehvar operator" must "use global defaults when specified but no " +
    "operator specific values hvar been defined." in
  {
    memory.clear
    memory += Constants.CurrentLStream -> "features1"
    memory += Constants.CurrentRStream -> "VerticesBySource"
    memory += "features1" + Constants.TupleType -> "EdgeType"
    memory += "features1" + Constants.NumKeys -> 1.toString
    memory += "features1" + Constants.KeyStr + 0 -> "SourceIp"
    memory += Constants.WindowSize -> "22"
    memory += Constants.EHK -> "5"
    parseAll(varOperator, "var(SrcTotalBytes)")
      match
    {
      case Success(matched,_) => 
         assert(matched.toString.contains(
           "std::make_shared<ExponentialHistogramVariance<"))
         assert(matched.toString.contains(
           "double, EdgeType, SrcTotalBytes, SourceIp>"))
         assert(matched.toString.contains("22, 5"))
      case Failure(msg,_) => assert(false)
      case Error(msg,_) => assert(false)
    }
  }

  "An var operator" must "use default values in Constants when no" +
  " global defaults hvar been specified and it should use the " +
  "ExponentialHistogramVar class" in {
    memory.clear
    memory += Constants.CurrentLStream -> "features1"
    memory += Constants.CurrentRStream -> "VerticesBySource"
    memory += "features1" + Constants.TupleType -> "EdgeType"
    memory += "features1" + Constants.NumKeys -> 1.toString
    memory += "features1" + Constants.KeyStr + 0 -> "SourceIp"
    parseAll(varOperator, "var(SrcTotalBytes)")
      match
    {
      case Success(matched,_) => 
         assert(matched.toString.contains(
           "std::make_shared<ExponentialHistogramVariance<"))
         assert(matched.toString.contains(
           "double, EdgeType, SrcTotalBytes, SourceIp>"))
         assert(matched.toString.contains(Constants.DefaultWindowSize + 
           ", " + Constants.DefaultEHK))
      case Failure(msg,_) => assert(false)
      case Error(msg,_) => assert(false)
    }
  }

  "An var operator" must "use global defaults when specified but no " +
    "operator specific values hvar been defined and it should use the " +
    "ExponentialHistogramVar class"  in
  {
    memory.clear
    memory += Constants.CurrentLStream -> "features1"
    memory += Constants.CurrentRStream -> "VerticesBySource"
    memory += "features1" + Constants.TupleType -> "EdgeType"
    memory += "features1" + Constants.NumKeys -> 1.toString
    memory += "features1" + Constants.KeyStr + 0 -> "SourceIp"
    memory += Constants.WindowSize -> "22"
    memory += Constants.EHK -> "5"
    parseAll(varOperator, "var(SrcTotalBytes)")
      match
    {
      case Success(matched,_) => 
         assert(matched.toString.contains(
           "std::make_shared<ExponentialHistogramVariance<"))
         assert(matched.toString.contains(
           "double, EdgeType, SrcTotalBytes, SourceIp>"))
         assert(matched.toString.contains("22, 5"))
      case Failure(msg,_) => assert(false)
      case Error(msg,_) => assert(false)
    }
  }
}
