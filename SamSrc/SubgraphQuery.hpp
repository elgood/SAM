#ifndef SAM_SUBGRAPH_QUERY_HPP
#define SAM_SUBGRAPH_QUERY_HPP

#include <map>
#include <set>
#include <stdexcept>
#include <list>
#include <boost/tokenizer.hpp>
#include <boost/lexical_cast.hpp>
#include <iostream>
#include <limits>
#include <iterator>
#include <algorithm>

#define MAX_START_END_OFFSET 100

namespace sam {

/**
 * These are the operators that can be used when describing a condition on an
 * node.
 *  
 * Equal example:
 * vertex1 = "192.168.0.1"
 * This specifies that we are looking for a particular node with the given id.
 *
 * In example:
 * vertex1 in top1000
 * Assuming top1000 defines the 1000 most frequent keys of a topk feature,
 * then this example specifies that the source must be one of the 1000 most
 * frequent keys.
 *
 * NotIn example:
 * vertex1 not in top1000
 * The source vertex must not be one of the 1000 most frequent keys.
 */
enum class NodeOperator {
  Equal,
  In,
  NotIn
};

/**
 * These are the operators that are defined for describing conditions on an
 * edge.
 *
 * Examples:
 * starttime(e2) > 0;
 *
 */
enum class EdgeOperator {
  LessThan,
  LessThanEqual,
  GreaterThan,
  GreaterThanEqual,
  Assignment,
  Equal
};

inline const std::string toString(EdgeOperator e)
{
  switch(e)
  {
    case EdgeOperator::LessThan: return "<";
    case EdgeOperator::LessThanEqual: return "<=";
    case EdgeOperator::GreaterThan: return ">";
    case EdgeOperator::GreaterThanEqual: return "<=";
    case EdgeOperator::Assignment: return "=";
    case EdgeOperator::Equal: return "==";
    default: return "Unknown edge operator";
  }
}

/**
 * List of functions that can be applied to edges.  Examples:
 * startime(e1) < 10: Extracts the start time of the edge and satisfies
 *   the condition if the starttime of the edge is within 10 seconds
 *   of the relative starttime.
 */
enum class EdgeFunction
{
  StartTime,
  EndTime
};

inline const std::string toString(EdgeFunction e)
{
  switch(e)
  {
    case EdgeFunction::StartTime: return "starttime";
    case EdgeFunction::EndTime: return "endtime";
    default: return "Unknown edge function";
  }
}

class BaseExpression {
public:
  virtual std::string toString() = 0;
};

class EdgeExpression : public BaseExpression {
public:
  std::string source;
  std::string edgeId;
  std::string target;  
  EdgeExpression(std::string source, std::string edgeId, std::string target)
  {
    this->source = source;
    this->edgeId = edgeId;
    this->target = target;
  }

  std::string toString() {
    return source + " " + edgeId + " " + target;   
  }

};

class TimeEdgeExpression : public BaseExpression {
public:
  EdgeFunction function;
  std::string edgeId;
  EdgeOperator op;
  double value;
  TimeEdgeExpression(EdgeFunction function, 
                     std::string edgeId, 
                     EdgeOperator op, 
                     double value)
  {
    this->function = function;
    this->edgeId = edgeId;
    this->op = op;
    this->value = value;
  }


  std::string toString() {
    return ::sam::toString(function) + "(" + edgeId + ") " + 
           ::sam::toString(op) + " " +
           boost::lexical_cast<std::string>(value);   
  }
};

class EdgeDescription
{ 
public:
  std::string source = ""; ///> The source of the edge
  std::string edgeId = ""; ///> Edge identifer
  std::string target = ""; ///> The target of the edge
 
  // By what time the edge needs to have started.
  double startTime = std::numeric_limits<double>::max();

  // By what time the edge needs to be done.
  double endTime = std::numeric_limits<double>::min(); 

  EdgeDescription() {}

  EdgeDescription(std::string source,
                  std::string edgeId,
                  std::string target)
  {
    this->source = source;
    this->edgeId = edgeId;
    this->target = target;
  }

  bool unspecifiedSource() {
    if (source.compare("") == 0)
      return true;
    else
      return false;
  }

  bool unspecifiedTarget() {
    if (target.compare("") == 0)
      return true;
    else
      return false;
  }

  bool unspecifiedStartTime() {
    if (startTime == std::numeric_limits<double>::max()) 
      return true;
    else
      return false;
  }

  bool unspecifiedEndTime() {
    if (endTime == std::numeric_limits<double>::min()) 
      return true;
    else
      return false;
  }

  std::string toString() {
    std::string rString = source + " " + edgeId + " " + target + " " +
                          boost::lexical_cast<std::string>(startTime) + " " +
                          boost::lexical_cast<std::string>(endTime);
    return rString;
  }


};

class SubgraphQueryException : public std::runtime_error
{
public:
  SubgraphQueryException(char const * message) : std::runtime_error(message) {}
  SubgraphQueryException(std::string message) : std::runtime_error(message) {}
};


class SubgraphQuery {
public:
  typedef std::vector<EdgeDescription> EdgeList; 
  typedef EdgeList::iterator iterator;
  typedef EdgeList::const_iterator const_iterator;

private:
  std::map<std::string, EdgeDescription> edges;
  EdgeList sortedEdges; //Sorted on startTime
  
  // Max time between start and end time.
  double maxOffset = MAX_START_END_OFFSET; 

public:

  iterator begin() { return sortedEdges.begin(); }
  iterator end() { return sortedEdges.end(); }
  const_iterator begin() const { return sortedEdges.begin(); }
  const_iterator end() const { return sortedEdges.end(); }

  void addExpression(TimeEdgeExpression expression);
  void addExpression(EdgeExpression expression);

  void finalize();

  double getMaxOffset() const { return maxOffset; }
  void setMaxOffset(double offset) { 
    if (offset < 0) {
      std::string message = "Tried to set offset to negative number " +
        boost::lexical_cast<std::string>(offset);
      throw SubgraphQueryException(message);
    }
    this->maxOffset = offset; 
  }

};

void SubgraphQuery::finalize()
{
  // Confirm that all edges have a start time or end time
  for (auto keypair : edges) {


    std::string key = keypair.first;
    EdgeDescription edge = keypair.second;

    // Check to make sure we have a source and dest
    if (edge.unspecifiedSource() || edge.unspecifiedTarget()) {
      std::string message = "In trying to finalize list of edges, found " 
        "an edge that does not have a source and/or target";
      throw SubgraphQueryException(message);  
    }


    if (edge.unspecifiedStartTime() &&
        edge.unspecifiedEndTime())
    {
      std::string message = "Both starttime and endtime for edge " +
        key + " are not defined";
      throw SubgraphQueryException(message);
    } else if (!edge.unspecifiedStartTime() &&
                edge.unspecifiedEndTime())
    { 
      edges[key].endTime = edge.startTime + maxOffset;
    } else if (edge.unspecifiedStartTime() &&
               !edge.unspecifiedEndTime())
    {
      edges[key].startTime = edge.endTime - maxOffset;
      if (edges[key].startTime < 0) {
        std::cout << "Warning: Setting starttime for edge " << key <<
          " to be zero because endtime -offset was negative." << std::endl;
        edges[key].startTime = 0;
      }
    }

    // Check to make sure the edges are valid.
    if (edges[key].startTime < 0) {
      std::string message = "Starttime for edge " + key + " is negative: " +
        boost::lexical_cast<std::string>(edges[key].startTime);  
    }
    if (edges[key].endTime < 0) {
      std::string message = "endtime for edge " + key + " is negative: " +
        boost::lexical_cast<std::string>(edges[key].startTime);  
    }
  }

  // TODO Need to add other constaints defined on edges

  transform(edges.begin(), edges.end(), 
            back_inserter(sortedEdges), [](auto val){ return val.second;}); 

  sort(sortedEdges.begin(), sortedEdges.end(), 
    [](EdgeDescription const & i, EdgeDescription const & j){
      return i.startTime < j.startTime; 
    });
  
}

void SubgraphQuery::addExpression(TimeEdgeExpression expression)
{
  EdgeFunction function = expression.function;
  std::string edgeId = expression.edgeId;
  EdgeOperator op = expression.op;
  double value = expression.value;

  edges[edgeId].edgeId = edgeId;

  switch (function)
  {
    case (EdgeFunction::StartTime):
      if (op == EdgeOperator::Assignment) {
        edges[edgeId].startTime = value;
      } 
      else if (op == EdgeOperator::GreaterThan ||
               op == EdgeOperator::GreaterThanEqual)
      {
        edges[edgeId].startTime = value; 
      } 
      else if (op == EdgeOperator::LessThan ||
               op == EdgeOperator::LessThanEqual)
      {
        edges[edgeId].startTime = value - maxOffset;
        if (edges[edgeId].startTime < 0) {
          edges[edgeId].startTime = 0;
          std::cout << "Warning: Setting starttime for edge " << edgeId <<
            " to be zero because starttime -offset was negative. " << 
            "Expression: " << expression.toString() << std::endl;
        }
      }
      else {
        std::string message = "Operator not implemented in expression" +
          expression.toString();
        throw SubgraphQueryException(message);
      }
      break;
    case (EdgeFunction::EndTime):
      if (op == EdgeOperator::Assignment) {
        edges[edgeId].endTime = value;
      } else {
        std::string message = "Expected assignment operator in expression" +
          expression.toString();
        throw SubgraphQueryException(message);
      }
      break;
    default:
      std::string message = "Unexpected function in expression " + 
        expression.toString();
      throw SubgraphQueryException(message);
  }

}

void SubgraphQuery::addExpression(EdgeExpression expression)
{
  std::string source = expression.source;
  std::string edgeId = expression.edgeId;
  std::string target = expression.target;

  if (edges.count(edgeId) > 0) {
    if (edges[edgeId].unspecifiedSource()) {
      edges[edgeId].source = source;
    } else {
      if (edges[edgeId].source.compare(source) != 0) {
        std::string message = "When adding expression: " +
          expression.toString() + ", the source conflicts with the already" +
          " specified source " + edges[edgeId].source;
        throw SubgraphQueryException(message);
      }
    }
    if (edges[edgeId].unspecifiedTarget()) {
      edges[edgeId].target = target;
    } else {
      if (edges[edgeId].target.compare(target) != 0) {
        std::string message = "When adding expression: " +
          expression.toString() + ", the target conflicts with the already" +
          " specified target " + edges[edgeId].source;
        throw SubgraphQueryException(message);
      }
    }
  } else {
    EdgeDescription desc(source, edgeId, target);
    edges[edgeId] = desc;

  }
}

/*union NodeOrNodeFunction
{
  std::string nodeId;
  NodeFunctionExpression nodeFunction;
}
*/

/**
 * Examples 
 */
/*class NodeFunctionExpression
{
public:
  NodeFunction function;
  std::string nodeId;
};


class NodeCondition
{
public:
  NodeOrNodeFunction term1;
  NodeOperator op;
    

}
// "target e1 bait;
// endtime(e1) = 0;
// target e2 controller;
// starttime(e2) > 0;
// starttime(e2) < 10;
// bait in Top1000;
// controller not in Top1000;"

class EdgeCondition 
{
public:
  std::string 
  ComparisonOperator op;
  double value;

  EdgeCondition(ComparisonOperator op,
                double value)
  {
    this->op = op;
    this->value = value;
  }
};*/

/**
 * This specifies an edge along with the span of when the edge can exist.
 * The times are relative to the first edge.
 */
/*class EdgeDescription
{
public:
  std::string source; ///> The source of the edge
  std::string edgeId; ///> Edge identifer
  std::string target; ///> The target of the edge
  double startTime; ///> By what time the edge needs to have started.
  double endTime; ///> By what time the edge needs to be done.
  EdgeDescription(std::string source,
                  std::string edgeId,
                  std::string target,
                  double startTime,
                  double endTime)
  {
    this->source = source;
    this->edgeId = edgeId;
    this->target = target;
    this->startTime = startTime;
    this->endTime = endTime;
  }

}

class SubgraphQuery
{
private:
  std::map<std::string, std::list<std::string>> subgraph;
  
  std::vector<EdgeDescription> edgeDescriptions;

  std::map<std::string, std::list<EdgeCondition>> edgeConditions;

  void addClause(std::vector<std::string> const& terms); 
  bool isOperator(std::string s) const;
  ComparisonOperator whichOperator(std::string s) const;

public:
  SubgraphQuery(std::string subgraphDescription);

};

SubgraphQuery::SubgraphQuery(std::string subgraphDescription)
{
  // Expects a string like:
  // 
  // "target e1 bait;
  // endtime(e1) = 0;
  // target e2 controller;
  // starttime(e2) > 0;
  // starttime(e2) < 10;
  // bait in Top1000;
  // controller not in Top1000;"
  //
  // So each clause is separated by a semicolon.  We first tokenize
  // on the semicolons.

  boost::char_separator<char> sep(";");
  boost::tokenizer<boost::char_separator<char>> tokenizer(subgraphDescription,
                                                          sep);

  for (std::string clause : tokenizer) {
    
    // For each clause, get the terms  
    std::vector<std::string> terms;
    boost::tokenizer<> tok(clause);
    for (std::string term : tok) {
      terms.push_back(term); 
      addClause(terms);
    }
  }

}

bool SubgraphQuery::isOperator(std::string s) const
{
  if (s.compare("<") == 0 ||
      s.compare(">") == 0 ||
      s.compare("<=") == 0 ||
      s.compare(">=") == 0 ||
      s.compare("==") == 0)
  {
    return true;
  }
  return false;
}

ComparisonOperator SubgraphQuery::whichOperator(std::string s) const
{
  if (s.compare("<") == 0) 
    return LessThan;
  else if (s.compare(">") == 0) 
    return GreaterThan;
  else if (s.compare("<=") == 0) 
    return LessThanEqual;
  else if (s.compare(">=") == 0) 
    return GreaterThanEqual;
  else if (s.compare("==") == 0)
    return Equal;
  else {
    std::string message = "Undefined comparison operator: " + s; 
    throw SubgraphQueryException(message);
  }
}


void SubgraphQuery::addClause(std::vector<std::string> const& terms)
{
  if (terms.size() == 3) {
    if (isOperator(terms[1])) { //edge attribute
      std::string edge = terms[0];
      ComparisonOperator op = whichOperator(terms[1]);
      double value = boost::lexical_cast<double>(terms[2]);
      EdgeCondition edgeCondition(op, value);

      if (edgeConditions.count(edge) > 0) {
        edgeConditions[edge].push_back(edgeCondition);
      } else {
        std::list<EdgeCondition> l;
        l.push_back(edgeCondition);
        edgeConditions[edge] = l;
      }
    } else { // edge 
       
    }
  } else if (terms.size() == 4) {

  } else {
    std::string origClaus = "";
    std::ostringstream imploded;
    std::copy(terms.begin(), terms.end(),
                std::ostream_iterator<std::string>(imploded, " ")); 
    std::string message = "Encountered too many terms in clause " +
       imploded.str();
    throw SubgraphQueryException(message);
  }
}*/

}

#endif
