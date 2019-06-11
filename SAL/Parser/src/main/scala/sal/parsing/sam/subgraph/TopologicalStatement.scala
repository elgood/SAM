package sal.parsing.sam.subgraph

import sal.parsing.sam.BaseParsing
import com.typesafe.scalalogging.LazyLogging
import scala.collection.mutable.HashMap

trait Topological extends BaseParsing
{
  def topologicalStatement: Parser[TopologicalStatement] =
    identifier ~ identifier ~ identifier ~ ";" ^^
    {
      case source ~ edge ~ target ~ c =>
        TopologicalStatement(source, edge, target, memory)
    }
}

object TopologicalStatement { var counter = 0 }

case class TopologicalStatement(source: String,
                                edge: String,
                                target: String,
                                memory: HashMap[String, String])
extends SubgraphStatement with LazyLogging
{
  override def toString = {

    // Find out how many total expressions have been seen by this
    // query.  The key into memory looks like "Subgraph<count>_NumKeys".
    // If the key doesn't exist, we get zero by default.
    //var totalExpressions = memory.getOrElse(Constants.Subgraph + 
    //  SubgraphQuery.count + "_" +  Constants.NumKeys, "0")

    // Create a variable name to be used in the C++ code.  It has the
    // form edgeExpression<current subgraph>_<current topo statement>
    val varname = "edgeExpression" + SubgraphQuery.counter + "_" + 
      TopologicalStatement.counter

    // Add the varname to memory so we can find it later.  The key
    // into memory looks like Subgraph<count>_<totalExpressions>.
    //memory += Constants.Subgraph + SubgraphQuery.count + "_" + 
    //  totalExpressions -> varname 
   
    // Update how many total expressions have been seen for thie current
    // subgraph query. 
    //totalExpressions = (totalExpressions.toInt + 1).toString
    //memory(Constants.Subgraph + Constants.NumKeys) = totalExpressions

    val currentQueryVar = "query" + SubgraphQuery.counter

    TopologicalStatement.counter += 1
    "  EdgeExpression " + varname + "(" +
    "\"" + source + "\", " +
    "\"" + edge + "\", " +
    "\"" + target + "\");\n" +
    "  " + currentQueryVar + "->addExpression(" + varname + ");\n"
  }
}
