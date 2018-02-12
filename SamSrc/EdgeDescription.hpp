#ifndef SAM_EDGE_DESCRIPTION_HPP
#define SAM_EDGE_DESCRIPTION_HPP

#include <boost/lexical_cast.hpp>

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

template <typename Tuple>
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

  bool unspecifiedSource() const {
    if (source.compare("") == 0)
      return true;
    else
      return false;
  }

  bool unspecifiedTarget() const {
    if (target.compare("") == 0)
      return true;
    else
      return false;
  }

  bool unspecifiedStartTime() const {
    if (startTime == std::numeric_limits<double>::max()) 
      return true;
    else
      return false;
  }

  bool unspecifiedEndTime() const {
    if (endTime == std::numeric_limits<double>::min()) 
      return true;
    else
      return false;
  }

  std::string toString() const {
    std::string rString = source + " " + edgeId + " " + target + " " +
                          boost::lexical_cast<std::string>(startTime) + " " +
                          boost::lexical_cast<std::string>(endTime);
    return rString;
  }

  std::string getSource() const { return source; }
  std::string getEdgeId() const { return edgeId; }
  std::string getTarget() const { return target; }

  /**
   * Returns true if the tuple satisifies the constraints laid out by 
   * this edge description.
   */
  bool satisfies(Tuple const& tuple) const {
    return true;
  }


};



}

#endif
