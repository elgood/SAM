package sal.parsing.sam.subgraph

import scala.collection.mutable.HashMap
import com.typesafe.scalalogging.LazyLogging
import sal.parsing.sam.BaseParsing
import sal.parsing.sam.Constants


trait Subgraph extends BaseParsing with SubgraphStatements with LazyLogging
{
  def subgraph = Parser[SubgraphQuery] { 
    logger.info("Subgraph.subgraph")
    subgraphKeyWord ~ onKeyWord ~ identifier ~ 
    withKeyWord ~ sourceKeyWord ~ "(" ~ identifier ~ ")" ~
    andKeyWord ~ targetKeyWord ~ "(" ~ identifier ~ ")" ~ "{" ~ 
    rep(subgraphStatement) ~ "}" ^^
    {
      case sgkw ~ okw ~ stream ~ wkw ~ skw ~ p1 ~ source ~ p2 ~ 
        and ~ twk ~ p3 ~ target ~ p4 ~
        c1 ~ subgraphStatements ~ c2 =>
      SubgraphQuery(stream, source, target, subgraphStatements,
                     memory)     
    }
  }

  def subgraphStatement = topologicalStatement

}

case class SubgraphQuery(stream: String,
                    source: String,
                    target: String,
                    subgraphStatements: List[SubgraphStatement],
                    memory: HashMap[String, String])
{
  override def toString = {
    
   
    val queryVar = "query" + SubgraphQuery.counter
    val graphStoreVar = "graphStore" + SubgraphQuery.counter

    var rString = ""
    rString += graphStoreType(stream, source, target, memory)
    rString += queryType
    rString += "  auto " + queryVar + 
      " = std::make_shared<SubgraphQueryType" + SubgraphQuery.counter + 
      ">(featureMap);\n\n"

    
    rString += "  auto " + graphStoreVar + " = " +
      "std::make_shared<GraphStoreType" + SubgraphQuery.counter + ">(\n" +
      "    numNodes, nodeId,\n" +
      "    hostnames, startingPort + numNodes,\n" +
      "    hwm, graphCapacity,\n" +
      "    tableCapacity, resultsCapacity,\n" +
      "    numSockets, numPullThreads, timeout,\n" +
      "    timeWindow, featureMap);\n\n" +
      "  if (printerLocation != \"\") {\n" + 
      "    std::shared_ptr<AbstractPrinterType> printer = \n" +
      "      std::make_shared<PrinterType>(printerLocation);\n"  +
      "    graphStore" + SubgraphQuery.counter + "->setPrinter(printer);\n" +
      "  }\n"

    val producer = memory(stream + Constants.VarName)
    rString += "  " + producer + "->registerConsumer(" + graphStoreVar +");\n\n"

    subgraphStatements.foreach{rString += _.toString }
    rString += "\n"

    rString += "  " + queryVar + "->finalize();\n"
    rString += "  " + graphStoreVar + "->registerQuery(" + queryVar + ");\n\n"

    // Update how many subgraph queries we have seen.
    SubgraphQuery.counter += 1

    rString

  }

  def graphStoreType(stream: String, source: String, target: String,
                     memory: HashMap[String, String]) = 
  {
    val tupleType    = memory(stream + Constants.TupleType)
    val tuplizerType = memory(stream + Constants.TuplizerType)
    val time         = memory(stream + Constants.TimeField)
    val duration     = memory(stream + Constants.DurationField)
    val sourceHash   = memory(stream + Constants.SourceHash)
    val targetHash   = memory(stream + Constants.TargetHash)
    val sourceEq     = memory(stream + Constants.SourceEq)
    val targetEq     = memory(stream + Constants.TargetEq)

    "  typedef GraphStore<" + tupleType + ", " + tuplizerType + ",\n" +
    "    " + source + ", " + target + ",\n" +
    "    " + time + ", " + duration + ",\n" +
    "    " + sourceHash + ", " + targetHash + ",\n" +
    "    " + sourceEq + ", " + targetEq + "> GraphStoreType" + 
    SubgraphQuery.counter + ";\n\n" +
    "  typedef AbstractSubgraphPrinter<" + tupleType + ", " + source + ",\n" +
    "    " + target + ", " + time + ", " + duration + ">\n" +
    "    AbstractPrinterType;\n" +
    "  typedef SubgraphDiskPrinter<" + tupleType + ", " + source + ",\n" +
    "    " + target + ", " + time + ", " + duration + ">\n" +
    "    PrinterType;\n"

  }

  def queryType = {
    "  typedef GraphStoreType" + SubgraphQuery.counter + "::QueryType " +
    "SubgraphQueryType" + SubgraphQuery.counter + ";\n\n"  
  }
}

/**
 * Keeps track of statics counters, i.e. the number of subgraph queries
 * and the number of expressions.  The expressions counter is reset
 * when a new query is parsed.
 */
object SubgraphQuery {
  var counter = 0
  var expressions = 0
}
