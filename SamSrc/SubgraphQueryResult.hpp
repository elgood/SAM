#ifndef SAM_SUBGRAPH_QUERY_RESULT_HPP
#define SAM_SUBGRAPH_QUERY_RESULT_HPP

#include "SubgraphQuery.hpp"
#include "Null.hpp"
#include "EdgeRequest.hpp"
#include "Util.hpp"
#include <set>

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
 * The source and target fields need to be of the same type.
 */
template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration>
class SubgraphQueryResult
{


public:
  typedef typename std::tuple_element<source, TupleType>::type SourceType;
  typedef typename std::tuple_element<target, TupleType>::type TargetType;
  typedef SourceType NodeType;
  typedef SubgraphQueryResult<TupleType, source, target, time, duration> 
    SubgraphQueryResultType;
  typedef SubgraphQuery<TupleType, time, duration> SubgraphQueryType;
  typedef EdgeDescription<TupleType, time, duration> EdgeDescriptionType;
  typedef EdgeRequest<TupleType, source, target> EdgeRequestType;
  

private:
  /// The SubgraphQuery that this is a result for.
  SubgraphQueryType const* subgraphQuery;

  /// A mapping from the variable name to the bound value.
  std::map<std::string, NodeType> var2BoundValue;

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

  /// Seen edges.  It is possible for the same edge to be presented to the
  /// same partial result.  For example, two edge requests can be produced
  /// return the same edge to this node.  When we try to map against the
  /// query result, the same edge will fulfill the same criteria twice.
  /// We want to prevent that.  We create a string from time,source,target,
  /// and then store them in this set so that we only see it once.
  std::set<std::string> seenEdges;

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
  addEdge(TupleType const& tuple);

  /// Tries to add the edge to the subgraph query result.
  /// \return Returns true if the netflow was added.  False otherwise.
  bool
  addEdgeInPlace(TupleType const& tuple);

  bool boundSource() const {return !sam::isNull(getCurrentSource()); }
  bool boundTarget() const {return !sam::isNull(getCurrentTarget()); }
    


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
   * a null value using nullValue<NodeType>() if there is no variable
   * binding for the source.
   */
  NodeType getCurrentSource() const; 
  
  /**
   * Returns the variable binding for the target of the current edge or
   * a null value using nullValue<NodeType>() if there is no variable
   * binding for the target.
   */
  NodeType getCurrentTarget() const; 

  double getCurrentStartTimeFirst() const;
  double getCurrentStartTimeSecond() const;
  double getCurrentEndTimeFirst() const;
  double getCurrentEndTimeSecond() const;

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
    #ifdef DEBUG
    printf("SubgraphQueryResult::complete() %s\n", toString().c_str());
    #endif

    return currentEdge == numEdges; 
  } 

  /**
   * Returns a string representation of the query result
   */
  std::string toString() const {
    if (resultEdges.size() != currentEdge) {
      std::string message = "resultEdges.size() was not equal to currentEdge " +
        boost::lexical_cast<std::string>(resultEdges.size()) + " != " +
        boost::lexical_cast<std::string>(currentEdge);
      throw SubgraphQueryResultException(message); 
    }
    std::string rString = "Result Edges: ";
    for(TupleType const& t : resultEdges) {
      rString = rString + " ResultTuple " + 
        "Id " + boost::lexical_cast<std::string>(std::get<0>(t)) +
        " Time " + boost::lexical_cast<std::string>(std::get<time>(t)) +
        " Duration " + boost::lexical_cast<std::string>(std::get<duration>(t)) +
        " Source " + boost::lexical_cast<std::string>(std::get<source>(t)) +
        " Target " + boost::lexical_cast<std::string>(std::get<target>(t));
      //rString = rString + "ResultTuple " + sam::toString(t) + " ";  
    }
    rString += " startTime" + boost::lexical_cast<std::string>(startTime);
    rString += " var2BoundValue ";
    for(auto key : var2BoundValue) {
      rString += key.first + "->" + key.second + " ";
    }
    rString += " currentEdge: " + boost::lexical_cast<std::string>(currentEdge);
    rString += " numEdges: " + boost::lexical_cast<std::string>(numEdges);
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

  /**
   * An instance of a SubgraphQueryResult is considered null if there is
   * no associated subgraphQuery.
   */
  bool isNull() const 
  {
    return subgraphQuery == nullptr;
  }

  TupleType getResultTuple(size_t i) const {
    return resultEdges[i];
  }

private:

  
  void addTimeInfoFromCurrent(EdgeRequestType & edgeRequest,
                              double previousStartTime) const;
  double getPreviousStartTime() const;
    
};

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration>
SubgraphQueryResult<TupleType, source, target, time, duration>::
SubgraphQueryResult(SubgraphQueryType const* query,
                    TupleType firstEdge) :
  subgraphQuery(query)
{
  DEBUG_PRINT("SubgraphQueryResult::SubgraphQueryResult(query, firstedge) "
    "Creating subgraphquery result, first edge: %s\n", 
    sam::toString(firstEdge).c_str());
  
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
void
SubgraphQueryResult<TupleType, source, target, time, duration>::
addTimeInfoFromCurrent(EdgeRequestType & edgeRequest,
                       double previousStartTime) const
{
  

  EdgeDescriptionType desc = subgraphQuery->getEdgeDescription(currentEdge);
  if (previousStartTime > desc.startTimeRange.first + startTime) {
    edgeRequest.setStartTimeFirst(previousStartTime);
  } else {
    edgeRequest.setStartTimeFirst(desc.startTimeRange.first + startTime);
  }
  edgeRequest.setStartTimeSecond(desc.startTimeRange.second + startTime);
  edgeRequest.setEndTimeFirst(desc.endTimeRange.first + startTime);
  edgeRequest.setEndTimeSecond(desc.endTimeRange.second + startTime);
}

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration>
double
SubgraphQueryResult<TupleType, source, target, time, duration>::
getPreviousStartTime() const
{
  if (resultEdges.size() > 0) {
    return std::get<time>(resultEdges.at(resultEdges.size() - 1));
  }
  return std::numeric_limits<double>::lowest();
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
  NodeType src = getCurrentSource();

  // Get the target that we are looking for.  If the target is unbound,
  // then the null value is returned.  Otherwise the target is bound to
  // a valud, and the next tuple must have that value for the target.
  NodeType trg = getCurrentTarget();

  double previousTime = getPreviousStartTime();

  EdgeDescriptionType desc = subgraphQuery->getEdgeDescription(currentEdge);

  #ifdef DEBUG
  printf("SubgraphQueryResult::hash currentEdge %lu start time range "
    "%f %f stop time range %f %f\n",
    currentEdge, desc.startTimeRange.first, desc.startTimeRange.second,
    desc.endTimeRange.first, desc.endTimeRange.second);
  printf("SubgraphQueryResult::hash src %s trg %s\n", src.c_str(), trg.c_str());
  #endif
  
  // Case when the source is unbound but the target is bound to a value. 
  if (sam::isNull(src) && !sam::isNull(trg)) {

    #ifdef DEBUG
    printf("SubgraphQueryResult::hash: source is unbound, target is bound to"
      " %s\n", trg.c_str());
    #endif 
  
    // If the target hashes to a different node, we need to make an edge
    // request to that node.  
    if (targetHash(trg) % numNodes != nodeId) {

      EdgeRequestType edgeRequest;
      edgeRequest.setTarget(trg);
      addTimeInfoFromCurrent(edgeRequest, previousTime);
      edgeRequest.setReturn(nodeId);

      // Add the edge request to the list of new edge requests.
      edgeRequests.push_back(edgeRequest);
    }

    // Returns a hash of the target so that it can be placed in the correct
    // bin in the SubgraphQueryResultMap.
    return targetHash(trg);

  } else 
  // Case when the target is unbound but the source is bound to a value.
  if (!sam::isNull(src) && sam::isNull(trg)) {

    #ifdef DEBUG
    printf("SubgraphQueryResult::hash: target is unbound, source is bound to"
      " %s\n", src.c_str());
    #endif 

    // If the source hashes to a different node, we need to make an edge
    // request to that node.
    #ifdef DEBUG
    printf("SubgraphQueryResult::hash: sourceHash %llu %llu "
      " numNodes %lu nodeId %lu\n", sourceHash(src), sourceHash(src) % numNodes,
      numNodes, nodeId); 
    #endif
    if (sourceHash(src) % numNodes != nodeId) {


      #ifdef DEBUG

      #endif

      EdgeRequestType edgeRequest;
      edgeRequest.setSource(src);
      addTimeInfoFromCurrent(edgeRequest, previousTime);
      edgeRequest.setReturn(nodeId);

      // Add the edge request to the list of new edge requests.
      edgeRequests.push_back(edgeRequest);

    }

    // Returns a hash of the source so that it can be placed in the correct
    // bin in the SubgraphQueryResultMap.
    return sourceHash(src);

  } else 
  // Case when the target and source are both bound to a value.
  if (!sam::isNull(src) && !sam::isNull(trg)) {

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
      addTimeInfoFromCurrent(edgeRequest, previousTime);
      edgeRequest.setReturn(nodeId);
      edgeRequests.push_back(edgeRequest);
    }

    // Returns a hash of the source combined with the hash of the target 
    // so that it can be placed in the correct
    // bin in the SubgraphQueryResultMap.
    return sourceHash(src) * targetHash(trg);

  } else {
    std::string message = "SubgraphQueryResult::hash When trying "
      "to calculate the hash, both source and target are unbound.  ";
    try {
      message += "Current edge: " + 
        boost::lexical_cast<std::string>(currentEdge);
    } catch (std::exception e) {
      std::string message = "Trouble with lexical cast: " + 
        std::string(e.what());
      throw SubgraphQueryResultException(message);
    }
    try {
      message += " Num edges: " + boost::lexical_cast<std::string>(numEdges);
      message += " QueryResult as is " + toString();
    } catch (std::exception e) {
      std::string message = "Trouble with lexical cast: " + 
        std::string(e.what());
      throw SubgraphQueryResultException(message);
    }
    
    auto description = subgraphQuery->getEdgeDescription(
                            currentEdge);
    message += " Current EdgeDescription: " + description.toString(); 

    throw SubgraphQueryResultException(message);
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
  if (resultEdges.size() != currentEdge) {
    std::string message = "addEdgeInPlace resultEdges.size() was not equal"
      " to currentEdge " +
      boost::lexical_cast<std::string>(resultEdges.size()) + " != " +
      boost::lexical_cast<std::string>(currentEdge);
    throw SubgraphQueryResultException(message); 
  }

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

  DEBUG_PRINT("addEdgeInPlace src %s trg %s\n", src.c_str(), trg.c_str());

  // Case when the source has been bound but the target has not
  if (var2BoundValue.count(src) > 0 &&
      var2BoundValue.count(trg) == 0)
  {
    
    NodeType edgeSource = std::get<source>(edge);
    
    if (edgeSource == var2BoundValue[src]) {
      
      NodeType edgeTarget = std::get<target>(edge);
      var2BoundValue[trg] = edgeTarget;
      
    } else {
      return false;
    } 

  } else if (var2BoundValue.count(src) == 0 && 
             var2BoundValue.count(trg) > 0) {
    
    NodeType edgeTarget = std::get<target>(edge);
    
    if (edgeTarget == var2BoundValue[trg]) {
      
      NodeType edgeSource = std::get<source>(edge);
      var2BoundValue[src] = edgeSource;
        
    } else {
      return false;
    } 
  } else if (var2BoundValue.count(src) == 0 &&
             var2BoundValue.count(trg) == 0)
  { 
    #ifdef DEBUG
    printf("Setting %s->%s and %s->%s\n", src.c_str(), 
      std::get<source>(edge).c_str(), trg.c_str(), 
      std::get<target>(edge).c_str());
    #endif

    var2BoundValue[src] = std::get<source>(edge);
    var2BoundValue[trg] = std::get<target>(edge);
  } else {

    NodeType edgeTarget = std::get<target>(edge);
    NodeType edgeSource = std::get<target>(edge);

    if (edgeTarget != var2BoundValue[trg] ||
        edgeSource != var2BoundValue[src])
    {
      return false;
    }
  }

  resultEdges.push_back(edge);
  currentEdge++;

  #ifdef DEBUG
  printf("Add edge in place returning true\n");
  #endif

  std::string edgeKey = 
    boost::lexical_cast<std::string>(std::get<source>(edge)) +
    boost::lexical_cast<std::string>(std::get<target>(edge)) +
    boost::lexical_cast<std::string>(std::get<time>(edge)) +
    boost::lexical_cast<std::string>(std::get<duration>(edge));
  seenEdges.insert(edgeKey);
  return true;
}

template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration>
std::pair<bool, SubgraphQueryResult<TupleType, source, target, time, duration>> 
SubgraphQueryResult<TupleType, source, target, time, duration>::
addEdge(TupleType const& edge)
{
  // Create a string from the source, target, time, and duration.  Check to see
  // if that is in seenEdges.  If not, add it and continue processing.  If
  // so, do not continue processing.  This prevents duplicate subgraphs to
  // be created.
  std::string edgeKey = 
    boost::lexical_cast<std::string>(std::get<source>(edge)) +
    boost::lexical_cast<std::string>(std::get<target>(edge)) +
    boost::lexical_cast<std::string>(std::get<time>(edge)) +
    boost::lexical_cast<std::string>(std::get<duration>(edge));
  if (seenEdges.count(edgeKey) < 1) {

    seenEdges.insert(edgeKey);

    DEBUG_PRINT("SubgraphQueryResult::addEdge trying to add edge %s to result %s\n",
      sam::toString(edge).c_str(), toString().c_str());
      
    if (currentEdge >= numEdges) {
      std::string message = "SubgraphQueryResult::addEdge Tried to add an edge " 
        "but the query has already been satisfied, i.e. currentEdge(" + 
        boost::lexical_cast<std::string>(currentEdge) +
        ") >= numEdges (" + boost::lexical_cast<std::string>(numEdges) + ")";
      throw SubgraphQueryResultException(message);              
    }

    // We need to check if it fulfills the constraints of the edge description
    // and also it fits the existing variable bindings.

    if (currentEdge >= 1) {
      double previousTime = std::get<time>(resultEdges[currentEdge - 1]);
      double currentTime = std::get<time>(edge); 
      
      if (currentTime <= previousTime) {
        return std::pair<bool, SubgraphQueryResultType>(false, 
          SubgraphQueryResultType());
      }
    }

    // Checking against edge description constraints
    EdgeDescriptionType const& edgeDescription = 
      subgraphQuery->getEdgeDescription(currentEdge);

    if (!edgeDescription.satisfies(edge, this->startTime)) {

      DEBUG_PRINT("SubgraphQueryResult::addEdge this tuple %s did not satisfy this"
        " edgeDescription %s\n", sam::toString(edge).c_str(), 
        edgeDescription.toString().c_str());

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
    if (var2BoundValue.count(src) > 0 &&
        var2BoundValue.count(trg) == 0)
    {
      
      NodeType edgeSource = std::get<source>(edge);
      
      if (edgeSource == var2BoundValue.at(src)) {
        
        NodeType edgeTarget = std::get<target>(edge);
        newResult.var2BoundValue[trg] = edgeTarget;
        
      } else {

        DEBUG_PRINT("SubgraphQueryResult::addEdge: edgeSource %s "
          " did not match var2BoundValue.at(src) %s for tuple %s\n", 
          edgeSource.c_str(), var2BoundValue.at(src).c_str(),
          sam::toString(edge).c_str());
        return std::pair<bool, SubgraphQueryResultType>(false, 
          SubgraphQueryResultType());
      } 

    } else if (var2BoundValue.count(src) == 0 && 
               var2BoundValue.count(trg) > 0) 
    {
      
      NodeType edgeTarget = std::get<target>(edge);
      
      if (edgeTarget == var2BoundValue.at(trg)) {
        
        NodeType edgeSource = std::get<source>(edge);
        newResult.var2BoundValue[src] = edgeSource;
          
      } else {
        #ifdef DEBUG
        printf("SubgraphQueryResult::addEdge: edgeTarget %s "
          " did not match var2BoundValue.at(trg) %s for tuple %s\n", 
          edgeTarget.c_str(), var2BoundValue.at(trg).c_str(),
          sam::toString(edge).c_str());
        #endif

        return std::pair<bool, SubgraphQueryResultType>(false, 
          SubgraphQueryResultType());
      } 
    } else if (var2BoundValue.count(src) == 0 &&
               var2BoundValue.count(trg) == 0)
    { 
      //std::cout << "blah both 0 " << std::endl;
      newResult.var2BoundValue[src] = std::get<source>(edge);
      newResult.var2BoundValue[trg] = std::get<target>(edge);
    } else {
      
      NodeType edgeTarget = std::get<target>(edge);
      NodeType edgeSource = std::get<source>(edge);

      if (edgeTarget != var2BoundValue.at(trg) ||
          edgeSource != var2BoundValue.at(src))
      {
        #ifdef DEBUG
        printf("SubgraphQueryResult::addEdge: edgeTarget %s "
          " did not match var2BoundValue.at(trg) %s or edgeSource %s "
          " dis not match var2BoundValue.at(src) %s for tuple %s\n", 
          edgeTarget.c_str(), var2BoundValue.at(trg).c_str(),
          edgeSource.c_str(), var2BoundValue.at(src).c_str(),
          sam::toString(edge).c_str());
        #endif



        return std::pair<bool, SubgraphQueryResultType>(false,
          SubgraphQueryResultType());
      }

    }

    newResult.resultEdges.push_back(edge);
    newResult.currentEdge++;

    DEBUG_PRINT("SubgraphQueryResult::addEdge: Added edge %s, update query: %s\n",
      sam::toString(edge).c_str(), newResult.toString().c_str());

    return std::pair<bool, SubgraphQueryResultType>(true, newResult);
  } else {
    #ifdef DEBUG
    printf("SubgraphQueryResult::addEdge: Did not add edge %s to query %s"
      " because the edge had already been seen before.\n", 
      sam::toString(edge).c_str(), toString().c_str());
    #endif
    return std::pair<bool, SubgraphQueryResultType>(false, 
      SubgraphQueryResultType());
  }
}

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration>
typename SubgraphQueryResult<TupleType, source, 
                             target, time, duration>::NodeType 
SubgraphQueryResult<TupleType, source, target,
                    time, duration>::
getCurrentSource() const
{
  if (currentEdge >= numEdges) {
    std::string message = "SubgraphQueryResult::getCurrentSource "
      "Tried to access an edge that is past the index of numEdges: "
      "currentEdge " + boost::lexical_cast<std::string>(currentEdge) +
      " numEdges " + boost::lexical_cast<std::string>(numEdges);
    throw SubgraphQueryResultException(message);   
  }

  std::string sourceVar = subgraphQuery->getEdgeDescription(
                            currentEdge).getSource();
  std::string blah = "getCurrentSource var2BoundValue:\n";
  for(auto p : var2BoundValue) {
    blah = blah + p.first + "->" + p.second + "\n";
  }
  if (var2BoundValue.count(sourceVar) > 0) {
    return var2BoundValue.at(sourceVar);
  } else {
    return nullValue<NodeType>();
  }
}

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration>
typename SubgraphQueryResult<TupleType, source, target,
                             time, duration>::NodeType 
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
  if (var2BoundValue.count(targetVar) > 0) {
    return var2BoundValue.at(targetVar);
  } else {
    return nullValue<NodeType>();
  }
}

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration>
double
SubgraphQueryResult<TupleType, source, target,
                    time, duration>::
getCurrentStartTimeFirst() const
{
  if (currentEdge >= numEdges) {
    std::string message = "SubgraphQueryResult::getCurrentStartTimeFirst "
      "Tried to access an edge that is past the index of numEdges: "
      "currentEdge " + boost::lexical_cast<std::string>(currentEdge) +
      " numEdges " + boost::lexical_cast<std::string>(numEdges);
    throw SubgraphQueryResultException(message);   
  }

  return startTime + 
    subgraphQuery->getEdgeDescription(currentEdge).startTimeRange.first;
}

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration>
double
SubgraphQueryResult<TupleType, source, target,
                    time, duration>::
getCurrentStartTimeSecond() const
{
  if (currentEdge >= numEdges) {
    std::string message = "SubgraphQueryResult::getCurrentStartTimeSecond "
      "Tried to access an edge that is past the index of numEdges";
    throw SubgraphQueryResultException(message);   
  }

  return startTime + 
    subgraphQuery->getEdgeDescription(currentEdge).startTimeRange.second;
}

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration>
double
SubgraphQueryResult<TupleType, source, target,
                    time, duration>::
getCurrentEndTimeFirst() const
{
  if (currentEdge >= numEdges) {
    std::string message = "SubgraphQueryResult::getCurrentEndTimeFirst "
      "Tried to access an edge that is past the index of numEdges";
    throw SubgraphQueryResultException(message);   
  }

  return startTime + 
    subgraphQuery->getEdgeDescription(currentEdge).endTimeRange.first;
}

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration>
double
SubgraphQueryResult<TupleType, source, target,
                    time, duration>::
getCurrentEndTimeSecond() const
{
  if (currentEdge >= numEdges) {
    std::string message = "SubgraphQueryResult::getCurrentEndTimeSecond "
      "Tried to access an edge that is past the index of numEdges";
    throw SubgraphQueryResultException(message);   
  }

  return startTime + 
    subgraphQuery->getEdgeDescription(currentEdge).endTimeRange.second;
}




}

#endif
