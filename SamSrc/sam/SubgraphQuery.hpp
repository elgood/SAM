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
#include <type_traits>
#include <sam/EdgeDescription.hpp>
#include <sam/FeatureMap.hpp>
#include <sam/VertexConstraintChecker.hpp>

#define MAX_START_END_OFFSET 100

namespace sam {
class SubgraphQueryException : public std::runtime_error
{
public:
  SubgraphQueryException(char const * message) : std::runtime_error(message) {}
  SubgraphQueryException(std::string message) : std::runtime_error(message) {}
};

/**
 * This class represents a subgraph query.  The overall process is to
 * create a subgraph query with a constructor, add expressions to the query,
 * and then finalize it.  e.g.
 *
 * SubgraphQuery query;
 * query.addExpression(timeEdgeExpression);
 * query.addExpression(edgeExpression);
 * query.addExpression(vertexConstraintExpression);
 * query.finalize();
 * 
 * An exception is thrown if you try to add an expression after finalize 
 * has been called, e.g.
 * 
 * SubgraphQuery query;
 * query.finalize();
 * query.addExpression(timeEdgeExpression); //Throws exception
 *
 * The finalize takes the list of edge descriptions that have been built up
 * by the add addExpression methods, does some checks, and then sorts them
 * by starttime.
 */
template <typename TupleType, size_t source, size_t target,  
          size_t time, size_t duration>
class SubgraphQuery {
public:
  typedef EdgeDescription<TupleType, time, duration> EdgeDesc;
  typedef std::vector<EdgeDesc> EdgeList; 
  typedef typename EdgeList::iterator iterator;
  typedef typename EdgeList::const_iterator const_iterator;
  typedef SubgraphQuery<TupleType, source, target, time, duration> 
    SubgraphQueryType;
  typedef typename std::tuple_element<source, TupleType>::type SourceType;
  typedef typename std::tuple_element<source, TupleType>::type TargetType;
  typedef typename std::tuple_element<source, TupleType>::type NodeType;

private:
  
  /// A mapping from edge id (variable name) to the corresponding edge 
  /// description. 
  std::map<std::string, EdgeDesc> edges;

  /// A mapping from vertex id (variable name) to list of corresponding 
  /// vertex constraints
  std::map<std::string, std::list<VertexConstraintExpression>> 
    vertexConstraints;
  
  ///Sorted on startTime
  EdgeList sortedEdges; 
  
  /// Max time between start and end time.
  double maxOffset = MAX_START_END_OFFSET; 
  
  /// Indicates that finalized has been called.  
  /// If true, indicates that all expressions have been added to the query.
  bool finalized = false;

  /// The maximum amount of time between start time of the first edge to
  /// end start time of the last edge.
  double maxTimeExtent = 0;

  std::shared_ptr<const VertexConstraintChecker<SubgraphQueryType>> check;

  std::list<VertexConstraintExpression> emptyList;
public:
  
  /**
   * Constructor.
   */
  SubgraphQuery(std::shared_ptr<const FeatureMap> featureMap);

  std::string toString() const 
  {
    std::string rString = "";
    for (auto edge : sortedEdges) {
      rString += edge.toString() + " ";
    }
    return rString;
  }

  /**
   * Constant begin iterator to the vector of sorted edges.
   */
  const_iterator begin() const { return sortedEdges.begin(); }

  /**
   * Constant end iterator to the vector of sorted edges.
   */
  const_iterator end() const { return sortedEdges.end(); }

  std::list<VertexConstraintExpression> const& 
  getConstraints(std::string variable) const
  {
    DEBUG_PRINT("subgraphQuery::getConstraints variables %s\n", 
                variable.c_str());
    if (vertexConstraints.find(variable) != vertexConstraints.end()) {
      return vertexConstraints.at(variable);
    }
    return emptyList; 
  }

  /**
   * Returns a constant reference to the ith EdgeDescription in the
   * sorted list.
   */
  EdgeDesc const& getEdgeDescription(size_t index) const {
    return sortedEdges[index];
  }

  /**
   * Adds a TimeEdgeExpression to the subgraph query.  The TimeEdgeExpression
   * specifies start/end time for an edge.
   *
   * This method throws a SubgraphQueryException if the query has already
   * been finalized.
   */
  void addExpression(TimeEdgeExpression expression);

  /**
   * Adds an EdgeExpression to the subgraph query.  The EdgeExpression
   * specifies a source, an edge, and a target.
   *
   * This method throws a SubgraphQueryException if the query has already
   * been finalized.
   */
  void addExpression(EdgeExpression expression);

  /**
   * Adds a VertexConstraintExpression to the subgraph query.  The
   * VertexConstraintExpression specifies a constraint on the vertex
   * regarding an extracted feature.
   *
   * This method throws a SubgraphQueryException if the query has already
   * been finalized.
   */
  void addExpression(VertexConstraintExpression expression);

  /**
   * This is called after all the expressions have been added.  If sorts
   * the EdgeDescriptions by start time.  It also calculates the overall
   * time that the query can take.
   */
  void finalize();

  /**
   * Returns the maximum time difference in seconds between start and end times
   * of an edge.
   */
  double getMaxOffset() const { return maxOffset; }

  /**
   * Sets the maximum time difference in seconds between start and end times.
   */
  void setMaxOffset(double offset) { 
    if (finalized) {
      throw SubgraphQueryException("Tried to set max offset, but the query"
        " has already been finalized."); 
    }
    if (offset < 0) {
      std::string message = "Tried to set offset to negative number " +
        boost::lexical_cast<std::string>(offset);
      throw SubgraphQueryException(message);
    }
    this->maxOffset = offset; 
  }

  /**
   * Returns the number of edge descriptions.
   */
  size_t size() const;


  /**
   * Returns the maximum extent of time that can pass from the start
   * time of the first edge to the end time of the last edge.
   */ 
  double getMaxTimeExtent() const;

  /**
   * Returns whether the tuple satisfies the first edge description. 
   * \param tuple The Tuple under question.
   * \param featureMap Uses featureMap for vertex constraints.
   */
  //bool satisfiesFirst(TupleType const& tuple) const; 
                
                
  /**
   * Checks whether vertex and edge constraints are satisfied.
   * \param index Index of query edge
   * \param tuple The tuple under consideration.
   * \param queryStart The time that the query started.
   */  
  bool satisfiesConstraints(size_t index, TupleType const& tuple,
                            double queryStart) const;

  /**
   * Returns true if the query has been finalized.
   */ 
  bool isFinalized() const { return finalized; }

  /**
   * The start of the query can be defined relative to the starttime of the
   * first edge or the endtime of the first edge.  This returns true
   * if the start of the query is defined relative to the start time of
   * the first edge.
   */
  bool zeroTimeRelativeToStart() const;

private:
 
  /**
   * Checks whether the tuple satisfies any defined vertex constraints.
   * \param index Which edge are we considering.
   * \param tuple The tuple that we are checking to see if fulfills the 
   *   constraints.
   */
  bool satisfiesVertexConstraints(size_t index, TupleType const& tuple) const;

  /**
   * Checks whether the tuple satisfies any defined edge constraints.
   * \param index Which edge are we considering.
   * \param tuple The tuple that we are checking to see if fulfills the 
   *   constraints.
   * \param queryStart The time that the query started.
   */
  bool satisfiesEdgeConstraints(size_t index, TupleType const& tuple,
                                double queryStart) const;


};

// Constructor
template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration>
SubgraphQuery<TupleType, source, target, time, duration>::
SubgraphQuery(std::shared_ptr<const FeatureMap> featureMap)
{
  check = std::make_shared<const VertexConstraintChecker<SubgraphQueryType>>(
            featureMap, this);  
  static_assert(std::is_same<SourceType, TargetType>::value,
                "SourceType and TargetType must be the same.");
  static_assert(std::is_same<SourceType, NodeType>::value,
                "SourceType and NodeType must be the same.");
}

template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration>
bool 
SubgraphQuery<TupleType, source, target, time, duration>::
zeroTimeRelativeToStart() const
{
  if (sortedEdges[0].startTimeRange.first == 0) {
    return true;
  }
  if (sortedEdges[0].endTimeRange.second == 0) {
    return false;
  }
  throw SubgraphQueryException("Couldn't figure out relative start of query");
}

template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration>
bool SubgraphQuery<TupleType, source, target, time, duration>::
satisfiesConstraints(size_t index, TupleType const& tuple,
                     double startTime) const
{
  return satisfiesEdgeConstraints(index, tuple, startTime) &&
         satisfiesVertexConstraints(index, tuple);
}


template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration>
bool SubgraphQuery<TupleType, source, target, time, duration>::
satisfiesEdgeConstraints(size_t index, TupleType const& tuple, 
                         double startTime) const
{
  bool b = sortedEdges[index].satisfies(tuple, startTime);
  DEBUG_PRINT("satisfiesEdgeConstraints returning %d tuple %s\n",
               b, sam::toString(tuple).c_str());
  return b;
}


template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration>
bool SubgraphQuery<TupleType, source, target, time, duration>::
satisfiesVertexConstraints(size_t index, TupleType const& tuple) const
{
  std::string src = sortedEdges[index].getSource();
  std::string trg = sortedEdges[index].getTarget();
  NodeType edgeSource = std::get<source>(tuple);
  NodeType edgeTarget = std::get<target>(tuple);
  if (check->check(src, edgeSource)) {
    if (check->check(trg, edgeTarget)) {
       DEBUG_PRINT("satisfiesVertexConstraints returning true tuple %s\n",
                   sam::toString(tuple).c_str());
      return true;
    }
  }
  DEBUG_PRINT("satisfiesVertexConstraints returning false tuple %s\n",
               sam::toString(tuple).c_str());
  return false;
}

/*template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration>
bool SubgraphQuery<TupleType, source, target, time, duration>::
satisfiesFirst(TupleType const& tuple) const
{
  double startTime = std::get<time>(tuple);
  if (sortedEdges[0].satisfies(tuple, startTime, check))
  {
    if (satisfiesVertexConstraints(0, tuple)) {
      return true;
    }
  }
  return false; 
}*/

template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration>
size_t SubgraphQuery<TupleType, source, target, time, duration>::size() const 
{
  if (!finalized) {
    std::string message = "SubgraphQuery::size() Tried to get the size of the " 
      "edge descriptions, but finalize has not been called yet.";
    throw SubgraphQueryException(message);
  }
  return sortedEdges.size(); 
}

template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration>
double 
SubgraphQuery<TupleType, source, target, 
              time, duration>::getMaxTimeExtent() const
{
  if (!finalized) {
    std::string message = "SubgraphQuery::size() Tried to get the maxTimeExtent"
      "but finalize has not been called yet.";
    throw SubgraphQueryException(message);
  }

  return maxTimeExtent; 
}

template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration>
void SubgraphQuery<TupleType, source, target, time, duration>::finalize()
{
  // Confirm that all edges have a start time or end time
  for (auto keypair : edges) {

    std::string key = keypair.first;
    EdgeDesc& edge = edges[key];

    // Check to make sure we have a source and dest
    if (edge.unspecifiedSource() || edge.unspecifiedTarget()) {
      std::string message = "In trying to finalize list of edges, found " 
        "an edge that does not have a source and/or target";
      throw SubgraphQueryException(message);  
    }

    edge.fixTimeRange(maxOffset);
  }

  transform(edges.begin(), edges.end(), 
            back_inserter(sortedEdges), [](auto val){ return val.second;}); 

  sort(sortedEdges.begin(), sortedEdges.end(), 
    [](EdgeDesc const & i, 
       EdgeDesc const & j){
      return i.startTimeRange.first < j.startTimeRange.first; 
    });

  if (zeroTimeRelativeToStart()) {
    maxTimeExtent = sortedEdges[sortedEdges.size()-1].endTimeRange.second 
                    - sortedEdges[0].startTimeRange.first;
  } else {
    maxTimeExtent = sortedEdges[sortedEdges.size()-1].endTimeRange.second 
                    - sortedEdges[0].endTimeRange.first;

  }

  finalized = true;
}

template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration>
void SubgraphQuery<TupleType, source, target, time, duration>::
addExpression(TimeEdgeExpression expression)
{
  if (finalized) {
    std::string message = "SubgraphQuery::addExpression(TimeEdgeExpression)"
      " Tried to add TimeEdgeExpression but the query had already been "
      "finalized.";
    throw SubgraphQueryException(message);
  }
  EdgeFunction function = expression.function;
  std::string edgeId = expression.edgeId;
  EdgeOperator op = expression.op;
  double value = expression.value;

  edges[edgeId].edgeId = edgeId;

  switch (function)
  {
    case (EdgeFunction::StartTime):
      if (op == EdgeOperator::Assignment) {
        edges[edgeId].startTimeRange.first = value;
        edges[edgeId].startTimeRange.second = value;
      } 
      else if (op == EdgeOperator::GreaterThan ||
               op == EdgeOperator::GreaterThanEqual)
      {
        edges[edgeId].startTimeRange.first = value; 
      } 
      else if (op == EdgeOperator::LessThan ||
               op == EdgeOperator::LessThanEqual)
      {
        edges[edgeId].startTimeRange.second = value;
      }
      else {
        std::string message = "Operator not implemented in expression:" +
          expression.toString();
        throw SubgraphQueryException(message);
      }
      break;
    case (EdgeFunction::EndTime):
      if (op == EdgeOperator::Assignment) {
        edges[edgeId].endTimeRange.first = value;
        edges[edgeId].endTimeRange.second = value;
      } else if (op == EdgeOperator::GreaterThan ||
                 op == EdgeOperator::GreaterThanEqual)
      {
        edges[edgeId].endTimeRange.first = value;
      }
      else if (op == EdgeOperator::LessThan ||
                 op == EdgeOperator::LessThanEqual)
      {
        edges[edgeId].endTimeRange.second = value;
      }
      else {
        std::string message = "Operator not implemented in expression:" +
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

template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration>
void SubgraphQuery<TupleType, source, target, time, duration>::
addExpression(EdgeExpression expression)
{
  if (finalized) {
    std::string message = "SubgraphQuery::addExpression(TimeEdgeExpression)"
      " Tried to add TimeEdgeExpression but the query had already been "
      "finalized.";
    throw SubgraphQueryException(message);
  }
  std::string src = expression.source;
  std::string edgeId = expression.edgeId;
  std::string trg = expression.target;

  if (edges.count(edgeId) > 0) {
    if (edges[edgeId].unspecifiedSource()) {
      edges[edgeId].source = src;
    } else {
      if (edges[edgeId].source.compare(src) != 0) {
        std::string message = "When adding expression: " +
          expression.toString() + ", the source conflicts with the already" +
          " specified source " + edges[edgeId].source;
        throw SubgraphQueryException(message);
      }
    }
    if (edges[edgeId].unspecifiedTarget()) {
      edges[edgeId].target = trg;
    } else {
      if (edges[edgeId].target.compare(trg) != 0) {
        std::string message = "When adding expression: " +
          expression.toString() + ", the target conflicts with the already" +
          " specified target " + edges[edgeId].source;
        throw SubgraphQueryException(message);
      }
    }
  } else {
    EdgeDesc desc(src, edgeId, trg);
    edges[edgeId] = desc;

  }
}

template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration>
void SubgraphQuery<TupleType, source, target, time, duration>::
addExpression(VertexConstraintExpression expression)
{
  if (finalized) {
    std::string message = "SubgraphQuery::addExpression(TimeEdgeExpression)"
      " Tried to add TimeEdgeExpression but the query had already been "
      "finalized.";
    throw SubgraphQueryException(message);
  }

  std::string variable = expression.vertexId;
  if (vertexConstraints.count(variable) == 0) {
    vertexConstraints[variable] = std::list<VertexConstraintExpression>();
  }
  vertexConstraints[variable].push_back(expression);
}

}

#endif
