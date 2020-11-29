#ifndef SAM_SUBGRAPH_QUERY_RESULT_HPP
#define SAM_SUBGRAPH_QUERY_RESULT_HPP

#include <sam/SubgraphQuery.hpp>
#include <sam/Null.hpp>
#include <sam/EdgeRequest.hpp>
#include <sam/Util.hpp>
#include <sam/VertexConstraintChecker.hpp>
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
template <typename EdgeType, size_t source, size_t target, 
          size_t time, size_t duration>
class SubgraphQueryResult
{

public:
  typedef typename EdgeType::LocalTupleType TupleType;
  typedef typename std::tuple_element<source, TupleType>::type SourceType;
  typedef typename std::tuple_element<target, TupleType>::type TargetType;
  typedef SourceType NodeType;
  typedef SubgraphQueryResult<EdgeType, source, target, time, duration> 
    SubgraphQueryResultType;
  typedef SubgraphQuery<TupleType, source, target, time, duration> 
    SubgraphQueryType;
  typedef EdgeDescription<TupleType, time, duration> EdgeDescriptionType;
  typedef EdgeRequest<TupleType, source, target> EdgeRequestType;
  
private:
  /// The SubgraphQuery that this is a result for.
  std::shared_ptr<const SubgraphQueryType> subgraphQuery;

  /// A mapping from the variable name to the bound value.
  std::map<std::string, NodeType> var2BoundValue;

  /// A vector of edges that satisfied the edge descriptions.
  std::vector<EdgeType> resultEdges;

  /// Index to current edge we are trying to satisfy.
  size_t currentEdge = 0;

  /// How many edges are in the query
  size_t numEdges = 0;

  /// The time this query result expires (usually time in seconds since
  /// epoch).
  double expireTime = 0;

  /// The time that the query started (either starttime or endtime of 
  /// first edge)
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
   * The constructor assumes that the check on the first edge description
   * has been satisfied.
   *
   * \param query The SubgraphQuery we are trying to satisfy.
   * \param id The id of the edge being added.
   * \param firstEdge The first edge that satisfies the first edge description.
   */
  SubgraphQueryResult(std::shared_ptr<const SubgraphQueryType>  query, 
                      EdgeType firstEdge);

  /** 
   * Tries to add the edge to the subgraph query result. If successful
   * returns true along with the new query result.  This result remains
   * unchanged.  Returns false if adding doesn't work. 
   * \return Returns a pair where the first value is true if the netflow was 
   *   added.  False otherwise.  If it was added, the second value is the
   *   new result with the added edge.
   */
  std::pair<bool, SubgraphQueryResult<EdgeType, source, 
                                      target, time, duration>> 
  addEdge(EdgeType const& edge);

  /**
   * Tries to add the edge to the subgraph query result.  This is a public
   * method to make it easy to test, but it should actually be a private 
   * method.  It shouldn't be used outside of this class.  It is only 
   * used by the constructor to create the first edge in the result.
   * For existing results, when we add an edge, we want to keep the
   * existing result for other matches and also create a new result.
   * That is the purpose of the other addEdge method.
   * 
   * \return Returns true if the netflow was added.  False otherwise.
   */
  bool
  addEdgeInPlace(EdgeType const& edge);

  /**
   * Convenience method that returns true if the source variable has been bound
   * to an actual value.  The source variable is the source of the edge
   * we are currently trying to match.  
   */
  bool boundSource() const {return !sam::isNull(getCurrentSource()); }

  /**
   * Convenience method that returns true if the target variable has been bound
   * to an actual value.  The target variable is the target of the edge
   * we are currently trying to match.
   */
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
    DEBUG_PRINT("SubgraphQueryResult::complete() %s\n", toString().c_str());
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
    size_t i = 0;
    for(EdgeType const& edge : resultEdges) {
      TupleType t = edge.tuple;
      rString = rString + " ResultTuple " + 
        "Id " + boost::lexical_cast<std::string>(edge.id) +
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
    size_t i = 0;
    for (EdgeType const& edge : resultEdges) {
      if (edge.id == samId) {
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
    return subgraphQuery.get() == nullptr;
  }

  EdgeType getResultTuple(size_t i) const {
    return resultEdges[i];
  }

private:

  void addTimeInfoFromCurrent(EdgeRequestType & edgeRequest,
                              double previousStartTime) const;
  double getPreviousStartTime() const;
    
};

// Constructor
template <typename EdgeType, size_t source, size_t target,
          size_t time, size_t duration>
SubgraphQueryResult<EdgeType, source, target, time, duration>::
SubgraphQueryResult(std::shared_ptr<const SubgraphQueryType> query,
                    EdgeType firstEdge) : 
  subgraphQuery(query)
{
  DEBUG_PRINT("SubgraphQueryResult::SubgraphQueryResult(query, firstedge) "
    "Creating subgraphquery result, first edge: %s\n", 
    firstEdge.toString().c_str());

  //TODO: Maybe remove this to improve performance
  if (!query->isFinalized()) {
    throw SubgraphQueryResultException("Subgraph query passed to "
      "SubgraphQueryResult is not finalized.");
  }
 
  numEdges = subgraphQuery->size();

  // If the query has start time defined relative to the start of the edge,
  // we set start time to be the start of the first edge.  Otherwise,
  // we set the start time of the query to be the end time of the first edge. 
  if (subgraphQuery->zeroTimeRelativeToStart()) {
    startTime = std::get<time>(firstEdge.tuple);
  } else {
    startTime = std::get<time>(firstEdge.tuple) + 
                std::get<duration>(firstEdge.tuple);
  }
  expireTime = startTime + query->getMaxTimeExtent();
 
  if (!addEdgeInPlace(firstEdge)) {
    throw SubgraphQueryResultException("SubgraphQueryResult::"
      "SubgraphQueryResult(query, firstEdge) Tried to add edge in "
      "constructor and it failed.");
  }
}

template <typename EdgeType, size_t source, size_t target,
          size_t time, size_t duration>
SubgraphQueryResult<EdgeType, source, target, time, duration>::
SubgraphQueryResult()
{
  subgraphQuery = nullptr;
}

template <typename EdgeType, size_t source, size_t target,
          size_t time, size_t duration>
void
SubgraphQueryResult<EdgeType, source, target, time, duration>::
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

template <typename EdgeType, size_t source, size_t target,
          size_t time, size_t duration>
double
SubgraphQueryResult<EdgeType, source, target, time, duration>::
getPreviousStartTime() const
{
  if (resultEdges.size() > 0) {
    return std::get<time>(resultEdges.at(resultEdges.size() - 1).tuple);
  }
  return std::numeric_limits<double>::lowest();
}

template <typename EdgeType, size_t source, size_t target,
          size_t time, size_t duration>
template <typename SourceHF, typename TargetHF>
size_t
SubgraphQueryResult<EdgeType, source, target, time, duration>::
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

  DEBUG_PRINT("SubgraphQueryResult::hash currentEdge %lu start time range "
    "%f %f stop time range %f %f\n",
    currentEdge, desc.startTimeRange.first, desc.startTimeRange.second,
    desc.endTimeRange.first, desc.endTimeRange.second);
  DEBUG_PRINT("SubgraphQueryResult::hash src %s trg %s\n", src.c_str(), trg.c_str());
  
  // Case when the source is unbound but the target is bound to a value. 
  if (sam::isNull(src) && !sam::isNull(trg)) {

    DEBUG_PRINT("SubgraphQueryResult::hash: source is unbound, target is bound to"
      " %s\n", trg.c_str());
  
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

template <typename EdgeType, size_t source, size_t target,
          size_t time, size_t duration>
double
SubgraphQueryResult<EdgeType, source, target, time, duration>::
getExpireTime() const
{
  return expireTime;
}

template <typename EdgeType, size_t source, size_t target, 
          size_t time, size_t duration>
bool
SubgraphQueryResult<EdgeType, source, target, time, duration>::
addEdgeInPlace(EdgeType const& edge)
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
  if (!subgraphQuery->satisfiesConstraints(currentEdge, edge.tuple, startTime))
  {
    return false;
  }

  EdgeDescriptionType const& edgeDescription = 
    subgraphQuery->getEdgeDescription(currentEdge);
  std::string src = edgeDescription.getSource();
  std::string trg = edgeDescription.getTarget();

  // Case when the source has been bound but the target has not
  if (var2BoundValue.count(src) > 0 &&
      var2BoundValue.count(trg) == 0)
  {
    
    NodeType edgeSource = std::get<source>(edge.tuple);
    
    if (edgeSource == var2BoundValue[src]) {
      
      NodeType edgeTarget = std::get<target>(edge.tuple);
      var2BoundValue[trg] = edgeTarget;
      
    } else {
      return false;
    } 

  } else if (var2BoundValue.count(src) == 0 && 
             var2BoundValue.count(trg) > 0) {
    
    NodeType edgeTarget = std::get<target>(edge.tuple);
    
    if (edgeTarget == var2BoundValue[trg]) {
      
      NodeType edgeSource = std::get<source>(edge.tuple);
      var2BoundValue[src] = edgeSource;
        
    } else {
      return false;
    } 
  } else if (var2BoundValue.count(src) == 0 &&
             var2BoundValue.count(trg) == 0)
  { 
    DEBUG_PRINT("Setting %s->%s and %s->%s\n", src.c_str(), 
      std::get<source>(edge.tuple).c_str(), trg.c_str(), 
      std::get<target>(edge.tuple).c_str());

    var2BoundValue[src] = std::get<source>(edge.tuple);
    var2BoundValue[trg] = std::get<target>(edge.tuple);
  } else {

    NodeType edgeTarget = std::get<target>(edge.tuple);
    NodeType edgeSource = std::get<target>(edge.tuple);

    if (edgeTarget != var2BoundValue[trg] ||
        edgeSource != var2BoundValue[src])
    {
      return false;
    }
  }

  resultEdges.push_back(edge);
  currentEdge++;

  DEBUG_PRINT_SIMPLE("Add edge in place returning true\n");

  std::string edgeKey = 
    boost::lexical_cast<std::string>(std::get<source>(edge.tuple)) +
    boost::lexical_cast<std::string>(std::get<target>(edge.tuple)) +
    boost::lexical_cast<std::string>(std::get<time>(edge.tuple)) +
    boost::lexical_cast<std::string>(std::get<duration>(edge.tuple));
  seenEdges.insert(edgeKey);
  return true;
}

template <typename EdgeType, size_t source, size_t target, 
          size_t time, size_t duration>
std::pair<bool, SubgraphQueryResult<EdgeType, source, target, time, duration>> 
SubgraphQueryResult<EdgeType, source, target, time, duration>::
addEdge(EdgeType const& edge)
{
  // Create a string from the source, target, time, and duration.  Check to see
  // if that is in seenEdges.  If not, add it and continue processing.  If
  // so, do not continue processing.  This prevents duplicate subgraphs to
  // be created.
  std::string edgeKey = 
    boost::lexical_cast<std::string>(std::get<source>(edge.tuple)) +
    boost::lexical_cast<std::string>(std::get<target>(edge.tuple)) +
    boost::lexical_cast<std::string>(std::get<time>(edge.tuple)) +
    boost::lexical_cast<std::string>(std::get<duration>(edge.tuple));
  if (seenEdges.count(edgeKey) < 1) {

    seenEdges.insert(edgeKey);

    DEBUG_PRINT("SubgraphQueryResult::addEdge trying to add edge %s to result"
      " %s\n", sam::toString(edge.tuple).c_str(), toString().c_str());
      
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
      double previousTime = std::get<time>(resultEdges[currentEdge - 1].tuple);
      double currentTime = std::get<time>(edge.tuple); 
      
      if (currentTime <= previousTime) {
        return std::pair<bool, SubgraphQueryResultType>(false, 
          SubgraphQueryResultType());
      }
    }

    // Checking against edge description constraints
    //EdgeDescriptionType const& edgeDescription = 
    //  subgraphQuery->getEdgeDescription(currentEdge);

    if (!subgraphQuery->satisfiesConstraints(currentEdge, edge.tuple, 
      startTime)) 
    {
      DEBUG_PRINT("SubgraphQueryResult::addEdge this tuple %s did not satisfy"
        " this edge constraings\n", sam::toString(edge.tuple).c_str());
      return std::pair<bool, SubgraphQueryResultType>(false, 
          SubgraphQueryResultType());
    }


    /*if (!edgeDescription.satisfiesEdgeConstraints(
          currentEdge, this->startTime)) {

      DEBUG_PRINT("SubgraphQueryResult::addEdge this tuple %s did not satisfy"
        " this edgeDescription %s\n", sam::toString(edge).c_str(), 
        edgeDescription.toString().c_str());

      return std::pair<bool, SubgraphQueryResultType>(false, 
        SubgraphQueryResultType());
    }

    if (!subgraphQuery->satisfiesVertexConstraints(currentEdge, edge))
    {
      return std::pair<bool, SubgraphQueryResultType>(false,
        SubgraphQueryResultType()); 
    }*/
   
    /*std::string src = edgeDescription.getSource();
    std::string trg = edgeDescription.getTarget();

    VertexConstraintChecker<SubgraphQueryType> check(*featureMap, 
                                                     *subgraphQuery);
    if (!check(src, edgeSource)) {
      return std::pair<bool, SubgraphQueryResultType>(false,
        SubgraphQueryResultType()); 
    }
    if (!check(trg, edgeTarget)) {
      return std::pair<bool, SubgraphQueryResultType>(false,
        SubgraphQueryResultType()); 
    }*/

    SubgraphQueryResultType newResult(*this);

    EdgeDescriptionType const& edgeDescription = 
      subgraphQuery->getEdgeDescription(currentEdge);
    std::string src = edgeDescription.getSource();
    std::string trg = edgeDescription.getTarget();
    NodeType edgeSource = std::get<source>(edge.tuple);
    NodeType edgeTarget = std::get<target>(edge.tuple);

    // Case when the source has been bound but the target has not
    if (var2BoundValue.count(src) > 0 &&
        var2BoundValue.count(trg) == 0)
    {
      
      if (edgeSource == var2BoundValue.at(src)) {
        
        NodeType edgeTarget = std::get<target>(edge.tuple);
        newResult.var2BoundValue[trg] = edgeTarget;
        
      } else {

        DEBUG_PRINT("SubgraphQueryResult::addEdge: edgeSource %s "
          " did not match var2BoundValue.at(src) %s for tuple %s\n", 
          edgeSource.c_str(), var2BoundValue.at(src).c_str(),
          sam::toString(edge.tuple).c_str());
        return std::pair<bool, SubgraphQueryResultType>(false, 
          SubgraphQueryResultType());
      } 

    } else if (var2BoundValue.count(src) == 0 && 
               var2BoundValue.count(trg) > 0) 
    {
      
      if (edgeTarget == var2BoundValue.at(trg)) {
        
        NodeType edgeSource = std::get<source>(edge.tuple);
        newResult.var2BoundValue[src] = edgeSource;
          
      } else {
        DEBUG_PRINT("SubgraphQueryResult::addEdge: edgeTarget %s "
          " did not match var2BoundValue.at(trg) %s for tuple %s\n", 
          edgeTarget.c_str(), var2BoundValue.at(trg).c_str(),
          sam::toString(edge.tuple).c_str());

        return std::pair<bool, SubgraphQueryResultType>(false, 
          SubgraphQueryResultType());
      } 
    } else if (var2BoundValue.count(src) == 0 &&
               var2BoundValue.count(trg) == 0)
    { 
      newResult.var2BoundValue[src] = edgeSource; 
      newResult.var2BoundValue[trg] = edgeTarget;
    } else {
      
      if (edgeTarget != var2BoundValue.at(trg) ||
          edgeSource != var2BoundValue.at(src))
      {
        DEBUG_PRINT("SubgraphQueryResult::addEdge: edgeTarget %s "
          " did not match var2BoundValue.at(trg) %s or edgeSource %s "
          " dis not match var2BoundValue.at(src) %s for tuple %s\n", 
          edgeTarget.c_str(), var2BoundValue.at(trg).c_str(),
          edgeSource.c_str(), var2BoundValue.at(src).c_str(),
          sam::toString(edge.tuple).c_str());



        return std::pair<bool, SubgraphQueryResultType>(false,
          SubgraphQueryResultType());
      }

    }

    newResult.resultEdges.push_back(edge);
    newResult.currentEdge++;

    DEBUG_PRINT("SubgraphQueryResult::addEdge: Added edge %s, update query:"
      " %s\n", sam::toString(edge.tuple).c_str(), newResult.toString().c_str());

    return std::pair<bool, SubgraphQueryResultType>(true, newResult);
  } else {
    DEBUG_PRINT("SubgraphQueryResult::addEdge: Did not add edge %s to query %s"
      " because the edge had already been seen before.\n", 
      sam::toString(edge.tuple).c_str(), toString().c_str());
    return std::pair<bool, SubgraphQueryResultType>(false, 
      SubgraphQueryResultType());
  }
}

template <typename EdgeType, size_t source, size_t target,
          size_t time, size_t duration>
typename SubgraphQueryResult<EdgeType, source, 
                             target, time, duration>::NodeType 
SubgraphQueryResult<EdgeType, source, target,
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
  if (var2BoundValue.count(sourceVar) > 0) {
    return var2BoundValue.at(sourceVar);
  } else {
    return nullValue<NodeType>();
  }
}

template <typename EdgeType, size_t source, size_t target,
          size_t time, size_t duration>
typename SubgraphQueryResult<EdgeType, source, target,
                             time, duration>::NodeType 
SubgraphQueryResult<EdgeType, source, target,
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

template <typename EdgeType, size_t source, size_t target,
          size_t time, size_t duration>
double
SubgraphQueryResult<EdgeType, source, target,
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

template <typename EdgeType, size_t source, size_t target,
          size_t time, size_t duration>
double
SubgraphQueryResult<EdgeType, source, target,
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

template <typename EdgeType, size_t source, size_t target,
          size_t time, size_t duration>
double
SubgraphQueryResult<EdgeType, source, target,
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

template <typename EdgeType, size_t source, size_t target,
          size_t time, size_t duration>
double
SubgraphQueryResult<EdgeType, source, target,
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
