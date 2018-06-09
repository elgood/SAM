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
#include "EdgeDescription.hpp"

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
template <typename TupleType, size_t time, size_t duration>
class SubgraphQuery {
public:
  typedef EdgeDescription<TupleType, time, duration> EdgeDesc;
  typedef std::vector<EdgeDesc> EdgeList; 
  typedef typename EdgeList::iterator iterator;
  typedef typename EdgeList::const_iterator const_iterator;

private:
  
  /// A mapping from edge id to the corresponding edge description. 
  std::map<std::string, EdgeDesc> edges;
  
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

public:

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
   * Returns whether the tuple satisfies the edge description (without any
   * variable bindings).
   * \param tuple The Tuple under question.
   * \param index The index of the edge description in sortedEdges.
   * \param startTime The time that the edge begins
   */
  bool satisfies(TupleType const& tuple, size_t index, double startTime) const; 

  bool isFinalized() const { return finalized; }

};

template <typename TupleType, size_t time, size_t duration>
bool SubgraphQuery<TupleType, time, duration>::
satisfies(TupleType const& tuple, size_t index, double startTime) const
{
  return sortedEdges[index].satisfies(tuple, startTime);
}

template <typename TupleType, size_t time, size_t duration>
size_t SubgraphQuery<TupleType, time, duration>::size() const 
{
  if (!finalized) {
    std::string message = "SubgraphQuery::size() Tried to get the size of the " 
      "edge descriptions, but finalize has not been called yet.";
    throw SubgraphQueryException(message);
  }
  return sortedEdges.size(); 
}

template <typename TupleType, size_t time, size_t duration>
double SubgraphQuery<TupleType, time, duration>::getMaxTimeExtent() const
{
  if (!finalized) {
    std::string message = "SubgraphQuery::size() Tried to get the maxTimeExtent"
      "but finalize has not been called yet.";
    throw SubgraphQueryException(message);
  }

  return maxTimeExtent; 
}

template <typename TupleType, size_t time, size_t duration>
void SubgraphQuery<TupleType, time, duration>::finalize()
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

  // TODO Need to add other constaints defined on edges



  transform(edges.begin(), edges.end(), 
            back_inserter(sortedEdges), [](auto val){ return val.second;}); 

  sort(sortedEdges.begin(), sortedEdges.end(), 
    [](EdgeDesc const & i, 
       EdgeDesc const & j){
      return i.startTimeRange.first < j.startTimeRange.first; 
    });

  maxTimeExtent = sortedEdges[sortedEdges.size()-1].endTimeRange.second 
                  - sortedEdges[0].startTimeRange.first;

  finalized = true;
}

template <typename TupleType, size_t time, size_t duration>
void SubgraphQuery<TupleType, time, duration>::
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

template <typename TupleType, size_t time, size_t duration>
void SubgraphQuery<TupleType, time, duration>::
addExpression(EdgeExpression expression)
{
  if (finalized) {
    std::string message = "SubgraphQuery::addExpression(TimeEdgeExpression)"
      " Tried to add TimeEdgeExpression but the query had already been "
      "finalized.";
    throw SubgraphQueryException(message);
  }
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
    EdgeDesc desc(source, edgeId, target);
    edges[edgeId] = desc;

  }
}

}

#endif
