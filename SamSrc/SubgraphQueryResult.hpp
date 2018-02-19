#ifndef SAM_SUBGRAPH_QUERY_RESULT_HPP
#define SAM_SUBGRAPH_QUERY_RESULT_HPP

#include "SubgraphQuery.hpp"
#include "Netflow.hpp"
#include "Null.hpp"

namespace sam {

class SubgraphQueryResultException : public std::runtime_error {
public:
  SubgraphQueryResultException(char const * message) : 
    std::runtime_error(message) { }
  SubgraphQueryResultException(std::string message) : 
    std::runtime_error(message) { }
};

/**
 * This contains the data that satisfies a SubgraphQuery.  The overall
 * process is to create the SubgraphQueryResult with a constant reference
 * to the SubgraphQuery, and add edges iteratively until it the entire
 * SubgraphQuery is satisfied.  
 *
 * The SubgraphQueryResult class does not store time.  A determination is
 * made that an intermediate result cannot be fulfilled by calling the
 * isExpired function.  Management of deleting expired partial results
 * resides outside this class.
 *
 */
template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration>
class SubgraphQueryResult
{
public:
  typedef typename std::tuple_element<source, TupleType>::type SourceType;
  typedef typename std::tuple_element<target, TupleType>::type TargetType;
  
private:
  /// The SubgraphQuery that this is a result for.
  SubgraphQuery<TupleType> const& subgraphQuery;

  /// A mapping from the variable name to the bound value.
  std::map<std::string, SourceType> var2SourceValue;
  std::map<std::string, TargetType> var2TargetValue;

  /// A vector of edges that satisfied the edge descriptions.
  std::vector<TupleType> resultEdges;

  /// Index to current edge we are trying to satisfy.
  size_t currentEdge = 0;

  /// How many edges are in the query
  size_t numEdges = 0;

  /// The time this query result expires (usually time in seconds since
  /// epoch).
  double expireTime = 0;

  /// The time that the query started (starttime of first edge)
  double startTime = 0;


public:
  /**
   * The construstor assumes that the check on the first edge description
   * has been satisfied.
   *
   * \param query The SubgraphQuery we are trying to satisfy.
   * \param firstEdge The first edge that satisfies the first edge description.
   * \param timeOffset The time extent of the query.
   */
  SubgraphQueryResult(SubgraphQuery<TupleType> const& query, 
                      TupleType firstEdge);

  /// Tries to add the edge to the subgraph query result.
  /// \return Returns true if the netflow was added.  False otherwise.
  bool addEdge(TupleType const& tuple);

  /**
   * Returns true if the query has expired (meaning it can't be fulfilled
   * because the time constraint of the entire query has been violated).
   * This class is not responsible for storing the current time, thus
   * it is provided as a parameter to the function.
   */
  bool isExpired(double currentTime) const {
    if (currentTime > expireTime) return true;
    return false; 
  }

  /**
   * Returns the expire time of the query, which is the start time 
   * of the last edge.
   */
  double getExpireTime() const;

  /**
   * Returns the variable binding for the source of the current edge or
   * a null value using nullValue<SourceType>() if there is no variable
   * binding for the source.
   */
  SourceType getCurrentSource() const; 
  
  /**
   * Returns the variable binding for the target of the current edge or
   * a null value using nullValue<TargetType>() if there is no variable
   * binding for the target.
   */
  TargetType getCurrentTarget() const; 

  /**
   * We need to hash based on source if that is defined, target, if that
   * is defined, or both if both are defined.  This function looks at the
   * current edge (unprocessed) and computes the hash based on what is defined.
   */
  template <typename SourceHF, typename TargetHF>
  size_t hash(SourceHF const& sourceHash, TargetHF const& targetHash) const;

  /**
   * Returns true if the query has been satisfied.
   */
  bool complete() const {return currentEdge == numEdges; } 

private:

  /**
   * Perfoms the check on whether the edge fulfills the time
   * constraints.
   */
  bool fulfillsTimeConstraint(TupleType const& edge);
  
    
};

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration>
template <typename SourceHF, typename TargetHF>
size_t
SubgraphQueryResult<TupleType, source, target, time, duration>::
hash(SourceHF const& sourceHash, TargetHF const& targetHash) const
{
  SourceType src = getCurrentSource();
  TargetType trg = getCurrentTarget();

  if (isNull(src) && !isNull(trg)) {
    return targetHash(trg);
  } else if (!isNull(src) && isNull(trg)) {
    return sourceHash(src);
  } else if (!isNull(src) && !isNull(trg)) {
    return sourceHash(src) * targetHash(trg);
  } else {
    throw SubgraphQueryResultException("SubgraphQueryResult::hash When trying "
      "to calculate the hash, both source and target are unbound.");
  }
}

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration>
SubgraphQueryResult<TupleType, source, target, time, duration>::
SubgraphQueryResult(SubgraphQuery<TupleType> const& query,
                    TupleType firstEdge) :
  subgraphQuery(query)
{
  //TODO: Maybe remove this to improve performance
  if (!query.isFinalized()) {
    throw SubgraphQueryResultException("Subgraph query passed to "
      "SubgraphQueryResult is not finalized.");
  }
  numEdges = subgraphQuery.size();
  double currentTime = std::get<time>(firstEdge);
  startTime = currentTime; 
  expireTime = currentTime + query.getMaxTimeExtent();
  addEdge(firstEdge);
}

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration>
double
SubgraphQueryResult<TupleType, source, target, time, duration>::
getExpireTime() const
{
  return expireTime;
}


template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration>
bool SubgraphQueryResult<TupleType, source, target, time, duration>::
fulfillsTimeConstraint(TupleType const& edge)
{
  double edgeActualStartTime = std::get<time>(edge);
  double edgeActualEndTime = edgeActualStartTime + 
    std::get<duration>(edge);
  double constraintStartTime_beg = subgraphQuery.getEdgeDescription(
    currentEdge).startTimeRange.first + this->startTime;
  double constraintStartTime_end = subgraphQuery.getEdgeDescription(
    currentEdge).startTimeRange.second + this->startTime;
  double constraintEndTime_beg = subgraphQuery.getEdgeDescription(
    currentEdge).endTimeRange.first + this->startTime;
  double constraintEndTime_end = subgraphQuery.getEdgeDescription(
    currentEdge).endTimeRange.second + this->startTime;


  if (edgeActualStartTime >= constraintStartTime_beg &&
      edgeActualStartTime <= constraintStartTime_end &&
      edgeActualEndTime >= constraintEndTime_beg &&
      edgeActualEndTime <= constraintEndTime_end)
  {
    return true;
  }
  
  return false;
}

template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration>
bool SubgraphQueryResult<TupleType, source, target, time, duration>::
addEdge(TupleType const& edge)
{
  if (currentEdge >= numEdges) {
    std::string message = "SubgraphQueryResult::addEdge Tried to add an edge " 
      "but the query has already been satisfied, i.e. currentEdge(" + 
      boost::lexical_cast<std::string>(currentEdge) +
      ") >= numEdges (" + boost::lexical_cast<std::string>(numEdges) + ")";
    throw SubgraphQueryResultException(message);              
  }

  EdgeDescription<TupleType> const& edgeDescription = 
    subgraphQuery.getEdgeDescription(currentEdge);

  std::string src = edgeDescription.getSource();
  std::string trg = edgeDescription.getTarget();

  /// Making sure we fulfill the time constraints
  if (!fulfillsTimeConstraint(edge)) {
    return false;
  }
  // TODO Add check for other constraints

  // Case when the source has been bound but the target has not
  if (var2SourceValue.count(src) > 0 &&
      var2TargetValue.count(trg) == 0)
  {
    
    SourceType edgeSource = std::get<source>(edge);
    
    if (edgeSource == var2SourceValue[src]) {
      
      TargetType edgeTarget = std::get<target>(edge);
      var2TargetValue[trg] = edgeTarget;
      
    } else {
      return false;
    } 

  } else if (var2SourceValue.count(src) == 0 && 
             var2TargetValue.count(trg) > 0) {
    
    TargetType edgeTarget = std::get<target>(edge);
    
    if (edgeTarget == var2TargetValue[trg]) {
      
      SourceType edgeSource = std::get<source>(edge);
      var2SourceValue[src] = edgeSource;
        
    } else {
      return false;
    } 
  } else if (var2SourceValue.count(src) == 0 &&
             var2TargetValue.count(trg) == 0)
  {
    var2SourceValue[src] = std::get<source>(edge);
    var2TargetValue[trg] = std::get<target>(edge);
  } else {
    std::string message = "SubgraphQueryResult::addEdge Tried to add an edge"
      " but both the source and target have already been defined.  Make sure"
      " this is a valid use case.";
    throw SubgraphQueryResultException(message);
  }

  resultEdges.push_back( edge );
  currentEdge++;

  return true;
}

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration>
typename SubgraphQueryResult<TupleType, source, 
                             target, time, duration>::SourceType 
SubgraphQueryResult<TupleType, source, target,
                    time, duration>::
getCurrentSource() const
{
  if (currentEdge >= numEdges) {
    std::string message = "SubgraphQueryResult::getCurrentSource Tried to"
      " access an edge that is past the index of numEdges";
    throw SubgraphQueryResultException(message);   
  }

  std::string sourceVar = subgraphQuery.getEdgeDescription(
                            currentEdge).getSource();
  if (var2SourceValue.count(sourceVar) > 0) {
    return var2SourceValue[sourceVar];
  } else {
    return nullValue<SourceType>();
  }
}

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration>
typename SubgraphQueryResult<TupleType, source, target,
                             time, duration>::TargetType 
SubgraphQueryResult<TupleType, source, target,
                    time, duration>::
getCurrentTarget() const
{
  if (currentEdge >= numEdges) {
    std::string message = "SubgraphQueryResult::getCurrentTarget Tried to"
      " access an edge that is past the index of numEdges";
    throw SubgraphQueryResultException(message);   
  }

  std::string targetVar = subgraphQuery.getEdgeDescription(
                            currentEdge).getTarget();
  if (var2TargetValue.count(targetVar) > 0) {
    return var2TargetValue[targetVar];
  } else {
    return nullValue<TargetType>();
  }
}

}

#endif
