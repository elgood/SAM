#ifndef SAM_SUBGRAPH_QUERY_RESULT_HPP
#define SAM_SUBGRAPH_QUERY_RESULT_HPP

#include "SubgraphQuery.hpp"
#include "Null.hpp"
#include "EdgeRequest.hpp"

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
  typedef SubgraphQueryResult<TupleType, source, target, time, duration> 
    SubgraphQueryResultType;
  typedef SubgraphQuery<TupleType, time, duration> SubgraphQueryType;
  typedef EdgeDescription<TupleType, time, duration> EdgeDescriptionType;
  typedef EdgeRequest<TupleType, source, target> EdgeRequestType;
  
private:
  /// The SubgraphQuery that this is a result for.
  SubgraphQueryType const* subgraphQuery;

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
   * Default constructor.
   */
  SubgraphQueryResult();

  /**
   * The construstor assumes that the check on the first edge description
   * has been satisfied.
   *
   * \param query The SubgraphQuery we are trying to satisfy.
   * \param firstEdge The first edge that satisfies the first edge description.
   * \param timeOffset The time extent of the query.
   */
  SubgraphQueryResult(SubgraphQueryType const* query, 
                      TupleType firstEdge);

  /// Tries to add the edge to the subgraph query result. If successful
  /// returns true along with the new query result.  This result remains
  /// unchanged.  Returns false if adding doesn't work. 
  /// \return Returns true if the netflow was added.  False otherwise.
  std::pair<bool, SubgraphQueryResult<TupleType, source, 
                                      target, time, duration>> 
  addEdge(TupleType const& tuple) const;

  /// Tries to add the edge to the subgraph query result.
  /// \return Returns true if the netflow was added.  False otherwise.
  bool
  addEdgeInPlace(TupleType const& tuple);


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
   * Also, it adds an edgeRequest to the list parameter, edgeRequests, if
   * the edge we are looking for will be found on another node.
   * \param sourceHash The source hash function.
   * \param targetHash The target hash function (usually the same as the souce
   *                   hash function).
   * \param edgeRequests A reference to a list of edge requests.  If the next
   *                     edge to be satisfied will be found on another node,
   *                     then an edge request is added to the list.
   * \param nodeId The id of the node running this code.  Used to determine
   *               if the next edge will be sent to this node by the 
   *               partitioner (in ZeroMQPushPull).
   * \param numNodes The number of nodes in the cluster.  Used to determin
   *                 if the next edge will be sent to this node by the 
   *                 partitioner (in ZeroMQPushPull).
   */
  template <typename SourceHF, typename TargetHF>
  size_t hash(SourceHF const& sourceHash, 
              TargetHF const& targetHash,
              std::list<EdgeRequestType> & edgeRequests,
              size_t nodeId,
              size_t numNodes) 
              const;

  /**
   * Returns true if the query has been satisfied.
   */
  bool complete() const {
    //std::cout << "currentEdge " << currentEdge << " numEdges " << numEdges
    //          << std::endl;
    return currentEdge == numEdges; 
  } 

  /**
   * Returns a string representation of the query result
   */
  std::string toString() const {
    std::string rString = "Result Edges:\n";
    for(TupleType const& t : resultEdges) {
      rString = rString + "ResultTuple " + sam::toString(t) + "\n";  
    }
    return rString;
  }

  /**
   * Returns true if none of the result edges have the given sam id.
   */
  bool noSamId(size_t samId) 
  {
    for (TupleType const& t : resultEdges) {
      if (std::get<0>(t) == samId) {
        return false;
      }
    }
    return true;
  }

private:

  /**
   * Perfoms the check on whether the edge fulfills the time
   * constraints.
   */
  //bool fulfillsTimeConstraint(TupleType const& edge);
  
    
};

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration>
SubgraphQueryResult<TupleType, source, target, time, duration>::
SubgraphQueryResult(SubgraphQueryType const* query,
                    TupleType firstEdge) :
  subgraphQuery(query)
{
  //TODO: Maybe remove this to improve performance
  if (!query->isFinalized()) {
    throw SubgraphQueryResultException("Subgraph query passed to "
      "SubgraphQueryResult is not finalized.");
  }
  numEdges = subgraphQuery->size();
  double currentTime = std::get<time>(firstEdge);
  startTime = currentTime; 
  expireTime = currentTime + query->getMaxTimeExtent();
  if (!addEdgeInPlace(firstEdge)) {
    throw SubgraphQueryResultException("SubgraphQueryResult::"
      "SubgraphQueryResult(query, firstEdge) Tried to add edge in "
      "constructor and it failed.");
  }
}

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration>
SubgraphQueryResult<TupleType, source, target, time, duration>::
SubgraphQueryResult()
{
  subgraphQuery = nullptr;
}



template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration>
template <typename SourceHF, typename TargetHF>
size_t
SubgraphQueryResult<TupleType, source, target, time, duration>::
hash(SourceHF const& sourceHash, 
     TargetHF const& targetHash,
     std::list<EdgeRequestType> & edgeRequests,
     size_t nodeId,
     size_t numNodes) 
     const
{
  // Get the source that we are looking for.  If the source is unbound,
  // then the null value is returned.  Otherwise the source is bound to
  // a value, and the next tuple must have that value for the source.
  SourceType src = getCurrentSource();

  // Get the target that we are looking for.  If the target is unbound,
  // then the null value is returned.  Otherwise the target is bound to
  // a valud, and the next tuple must have that value for the target.
  TargetType trg = getCurrentTarget();

  //std::cout << "src " << src << " trg " << trg << std::endl;

  // Case when the source is unbound but the target is bound to a value. 
  if (isNull(src) && !isNull(trg)) {
  
    // If the target hashes to a different node, we need to make an edge
    // request to that node.  
    if (targetHash(trg) % numNodes != nodeId) {

      EdgeRequestType edgeRequest;
      edgeRequest.setTarget(trg);

      // Add the edge request to the list of new edge requests.
      edgeRequests.push_back(edgeRequest);
    }

    // Returns a hash of the target so that it can be placed in the correct
    // bin in the SubgraphQueryResultMap.
    return targetHash(trg);

  } else 
  // Case when the target is unbound but the source is bound to a value.
  if (!isNull(src) && isNull(trg)) {

    // If the source hashes to a different node, we need to make an edge
    // request to that node.
    if (sourceHash(src) % numNodes != nodeId) {

      EdgeRequestType edgeRequest;
      edgeRequest.setSource(src);

      // Add the edge request to the list of new edge requests.
      edgeRequests.push_back(edgeRequest);

    }

    // Returns a hash of the source so that it can be placed in the correct
    // bin in the SubgraphQueryResultMap.
    return sourceHash(src);

  } else 
  // Case when the target and source are both bound to a value.
  if (!isNull(src) && !isNull(trg)) {

    // Need to make edge requests if both the source or target
    // map to a different node.  If either one maps to this node,
    // then we will get the edge.
    if (sourceHash(src) % numNodes != nodeId &&
        targetHash(trg) % numNodes != nodeId)
    {
      // It doesn't matter which node we send the edge request to
      EdgeRequestType edgeRequest;
      edgeRequest.setSource(src);
      edgeRequest.setTarget(trg);
      edgeRequests.push_back(edgeRequest);
    }

    // Returns a hash of the source combined with the hash of the target 
    // so that it can be placed in the correct
    // bin in the SubgraphQueryResultMap.
    return sourceHash(src) * targetHash(trg);

  } else {
    throw SubgraphQueryResultException("SubgraphQueryResult::hash When trying "
      "to calculate the hash, both source and target are unbound.");
  }
}

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration>
double
SubgraphQueryResult<TupleType, source, target, time, duration>::
getExpireTime() const
{
  return expireTime;
}


/*template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration>
bool SubgraphQueryResult<TupleType, source, target, time, duration>::
fulfillsTimeConstraint(TupleType const& edge)
{
  double edgeActualStartTime = std::get<time>(edge);
  double edgeActualEndTime = edgeActualStartTime + 
    std::get<duration>(edge);
  double constraintStartTime_beg = subgraphQuery->getEdgeDescription(
    currentEdge).startTimeRange.first + this->startTime;
  double constraintStartTime_end = subgraphQuery->getEdgeDescription(
    currentEdge).startTimeRange.second + this->startTime;
  double constraintEndTime_beg = subgraphQuery->getEdgeDescription(
    currentEdge).endTimeRange.first + this->startTime;
  double constraintEndTime_end = subgraphQuery->getEdgeDescription(
    currentEdge).endTimeRange.second + this->startTime;

  //std::cout << "edgeActualStartTime " << edgeActualStartTime - 1.5197e9 << std::endl;
  //std::cout << "constraintStartTime_beg " << constraintStartTime_beg  - 1.5197e9 << std::endl;
  //std::cout << "constraintStartTime_end " << constraintStartTime_end  - 1.5197e9 << std::endl;
  //std::cout << "edgeActualEndTime " << edgeActualEndTime  - 1.5197e9 << std::endl;
  //std::cout << "constraintEndTime_beg " << constraintEndTime_beg  - 1.5197e9 << std::endl;
  //std::cout << "constraintEndTime_end " << constraintEndTime_end  - 1.5197e9 << std::endl;

  if (edgeActualStartTime >= constraintStartTime_beg &&
      edgeActualStartTime <= constraintStartTime_end &&
      edgeActualEndTime >= constraintEndTime_beg &&
      edgeActualEndTime <= constraintEndTime_end)
  {
    return true;
  }
  
  return false;
}*/

template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration>
bool
SubgraphQueryResult<TupleType, source, target, time, duration>::
addEdgeInPlace(TupleType const& edge)
{
  if (currentEdge >= numEdges) {
    std::string message = "SubgraphQueryResult::addEdge Tried to add an edge " 
      "but the query has already been satisfied, i.e. currentEdge(" + 
      boost::lexical_cast<std::string>(currentEdge) +
      ") >= numEdges (" + boost::lexical_cast<std::string>(numEdges) + ")";
    throw SubgraphQueryResultException(message);              
  }

  // We need to check if it fulfills the constraints of the edge description
  // and also it fits the existing variable bindings.

  // Checking against edge description constraints
  EdgeDescriptionType const& edgeDescription = 
    subgraphQuery->getEdgeDescription(currentEdge);

  if (!edgeDescription.satisfies(edge, this->startTime)) {
    return false;
  }


  std::string src = edgeDescription.getSource();
  std::string trg = edgeDescription.getTarget();

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
    //std::cout << "blah both 0 " << std::endl;
    var2SourceValue[src] = std::get<source>(edge);
    var2TargetValue[trg] = std::get<target>(edge);
  } else {
    std::string message = "SubgraphQueryResult::addEdge Tried to add an edge"
      " but both the source and target have already been defined.  Make sure"
      " this is a valid use case.";
    throw SubgraphQueryResultException(message);
  }

  resultEdges.push_back(edge);
  currentEdge++;

  return true;
}

template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration>
std::pair<bool, SubgraphQueryResult<TupleType, source, target, time, duration>> 
SubgraphQueryResult<TupleType, source, target, time, duration>::
addEdge(TupleType const& edge) const
{
  if (currentEdge >= numEdges) {
    std::string message = "SubgraphQueryResult::addEdge Tried to add an edge " 
      "but the query has already been satisfied, i.e. currentEdge(" + 
      boost::lexical_cast<std::string>(currentEdge) +
      ") >= numEdges (" + boost::lexical_cast<std::string>(numEdges) + ")";
    throw SubgraphQueryResultException(message);              
  }

  // We need to check if it fulfills the constraints of the edge description
  // and also it fits the existing variable bindings.

  // Checking against edge description constraints
  EdgeDescriptionType const& edgeDescription = 
    subgraphQuery->getEdgeDescription(currentEdge);

  if (!edgeDescription.satisfies(edge, this->startTime)) {
    //std::cout << "Failed edgeDescription satisfies " << std::endl;
    return std::pair<bool, SubgraphQueryResultType>(false, 
      SubgraphQueryResultType());
  }


  std::string src = edgeDescription.getSource();
  std::string trg = edgeDescription.getTarget();

  //std::cout << " about to check time constraint " << std::endl;
  /// Making sure we fulfill the time constraints
  //if (!fulfillsTimeConstraint(edge)) {
  //  std::cout << " failed time constraint " << std::endl;
  //  return false;
  //}
  // TODO Add check for other constraints

  SubgraphQueryResultType newResult(*this);

  // Case when the source has been bound but the target has not
  if (var2SourceValue.count(src) > 0 &&
      var2TargetValue.count(trg) == 0)
  {
    
    SourceType edgeSource = std::get<source>(edge);
    
    if (edgeSource == var2SourceValue.at(src)) {
      
      TargetType edgeTarget = std::get<target>(edge);
      newResult.var2TargetValue[trg] = edgeTarget;
      
    } else {
      std::cout << "Failed to match source variable binding " << std::endl;
      return std::pair<bool, SubgraphQueryResultType>(false, 
        SubgraphQueryResultType());
    } 

  } else if (var2SourceValue.count(src) == 0 && 
             var2TargetValue.count(trg) > 0) {
    
    TargetType edgeTarget = std::get<target>(edge);
    
    if (edgeTarget == var2TargetValue.at(trg)) {
      
      SourceType edgeSource = std::get<source>(edge);
      newResult.var2SourceValue[src] = edgeSource;
        
    } else {
      std::cout << "Failed to match target variable binding " << std::endl;
      return std::pair<bool, SubgraphQueryResultType>(false, 
        SubgraphQueryResultType());
    } 
  } else if (var2SourceValue.count(src) == 0 &&
             var2TargetValue.count(trg) == 0)
  { 
    //std::cout << "blah both 0 " << std::endl;
    newResult.var2SourceValue[src] = std::get<source>(edge);
    newResult.var2TargetValue[trg] = std::get<target>(edge);
  } else {
    std::string message = "SubgraphQueryResult::addEdge Tried to add an edge"
      " but both the source and target have already been defined.  Make sure"
      " this is a valid use case.";
    throw SubgraphQueryResultException(message);
  }

  newResult.resultEdges.push_back(edge);
  newResult.currentEdge++;

  return std::pair<bool, SubgraphQueryResultType>(true, newResult);
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

  std::string sourceVar = subgraphQuery->getEdgeDescription(
                            currentEdge).getSource();
  if (var2SourceValue.count(sourceVar) > 0) {
    return var2SourceValue.at(sourceVar);
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

  std::string targetVar = subgraphQuery->getEdgeDescription(
                            currentEdge).getTarget();
  if (var2TargetValue.count(targetVar) > 0) {
    return var2TargetValue.at(targetVar);
  } else {
    return nullValue<TargetType>();
  }
}

}

#endif
