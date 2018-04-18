#ifndef SAM_EDGE_DESCRIPTION_HPP
#define SAM_EDGE_DESCRIPTION_HPP

#include <boost/lexical_cast.hpp>
#include <cmath>

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

class EdgeDescriptionException : public std::runtime_error
{
public:
  EdgeDescriptionException(char const * message) :std::runtime_error(message) {}
  EdgeDescriptionException(std::string message) : std::runtime_error(message) {}
};

template <typename TupleType, size_t time, size_t duration>
class EdgeDescription
{
public:
  std::string source = ""; ///> The source of the edge
  std::string edgeId = ""; ///> Edge identifer
  std::string target = ""; ///> The target of the edge

  // The range of time values over which the start of this edge should occur.
  std::pair<double, double> startTimeRange;
    
  // The range of time value over which the end of this edge should occur.
  std::pair<double, double> endTimeRange;

  EdgeDescription() {
    startTimeRange.first = std::numeric_limits<double>::lowest();
    startTimeRange.second = std::numeric_limits<double>::max();
    endTimeRange.first = std::numeric_limits<double>::lowest();
    endTimeRange.second = std::numeric_limits<double>::max();
  }

  EdgeDescription(std::string source,
                  std::string edgeId,
                  std::string target)
  {
    startTimeRange.first = std::numeric_limits<double>::lowest();
    startTimeRange.second = std::numeric_limits<double>::max();
    endTimeRange.first = std::numeric_limits<double>::lowest();
    endTimeRange.second = std::numeric_limits<double>::max();
    this->source = source;
    this->edgeId = edgeId;
    this->target = target;
  }

  void fixTimeRange(double maxOffset) {
    bool eb = (endTimeRange.first != std::numeric_limits<double>::lowest());
    bool ee = (endTimeRange.second != std::numeric_limits<double>::max());
    bool sb = (startTimeRange.first != std::numeric_limits<double>::lowest());
    bool se = (startTimeRange.second != std::numeric_limits<double>::max());
    
    //printf("fixTimeRange eb %d ee %d sb %d se %d\n", eb,ee,sb,se);
    
    if (eb && ee && sb && se) {
      //do nothing
    } else if (eb && ee && sb && !se) {
      //do nothing
    } else if (eb && ee && !sb && se) {
      //do nothing
    } else if (eb && ee && !sb && !se) {
      startTimeRange.first = endTimeRange.first - maxOffset;
      startTimeRange.second = endTimeRange.second - maxOffset;
    } else if (eb && !ee && sb && se) {
    } else if (eb && !ee && sb && !se) {
      //do nothing
    } else if (eb && !ee && !sb && se) {
      //do nothing
    } else if (eb && !ee && !sb && !se) {
      startTimeRange.first = endTimeRange.first - maxOffset;
    } else if (!eb && ee && sb && se) {
      //do nothing
    } else if (!eb && ee && sb && !se) {
      //do nothing
    } else if (!eb && ee && !sb && se) {
      //do nothing
    } else if (!eb && ee && !sb && !se) {
      startTimeRange.first = endTimeRange.first - 2*maxOffset;
      startTimeRange.second = endTimeRange.second - maxOffset;
    } else if (!eb && !ee && sb && se) {
      endTimeRange.first = startTimeRange.first;
      endTimeRange.second = startTimeRange.second + maxOffset;
    } else if (!eb && !ee && sb && !se) {
      endTimeRange.first = startTimeRange.first;
    } else if (!eb && !ee && !sb && se) {
      endTimeRange.first = startTimeRange.second - maxOffset;
      endTimeRange.second = startTimeRange.second + maxOffset;
    } else if (!eb && !ee && !sb && !se) {
      throw EdgeDescriptionException("EdgeDescription::fixTimeRange "
        "No times are defined.");
    }
    

    fixEndTimeRange(maxOffset);
    fixStartTimeRange(maxOffset);
  }

  /**
   * Changes things from numeric limits to something within a small time 
   * window for the endTimeRange.
   * \param maxOffset The maximum length of the range. 
   */
  void fixEndTimeRange(double maxOffset) {
    if (endTimeRange.first == std::numeric_limits<double>::lowest() &&
        endTimeRange.second != std::numeric_limits<double>::max()) {
      endTimeRange.first = endTimeRange.second - maxOffset;
    } else if (endTimeRange.first != std::numeric_limits<double>::lowest() &&
        endTimeRange.second == std::numeric_limits<double>::max()) {
      endTimeRange.second = endTimeRange.first + maxOffset;
    } else if (endTimeRange.first != std::numeric_limits<double>::lowest() &&
        endTimeRange.second != std::numeric_limits<double>::max()) 
    {
      if (std::abs(endTimeRange.second - endTimeRange.first) > 2*maxOffset) {
        throw EdgeDescriptionException("EdgeDescription::fixEndTimeRange: "
          "Tried to fix endTimeRange but the range is larger than the offset.");
      }
    } else {
      throw EdgeDescriptionException("EdgeDescription::fixEndTimeRange: "
        "Neither end of the endTimeRange is defined.");
    }
  }

  /**
   * Changes things from numeric limits to something within a small time 
   * window for the startTimeRange.
   * \param maxOffset The maximum length of the range. 
   */
  void fixStartTimeRange(double maxOffset) {
    if (startTimeRange.first == std::numeric_limits<double>::lowest() &&
        startTimeRange.second != std::numeric_limits<double>::max()) {
      startTimeRange.first = startTimeRange.second - maxOffset;
    } else if (startTimeRange.first != std::numeric_limits<double>::lowest() &&
        startTimeRange.second == std::numeric_limits<double>::max()) {
      startTimeRange.second = startTimeRange.first + maxOffset;
    } else if (startTimeRange.first != std::numeric_limits<double>::lowest() &&
        startTimeRange.second != std::numeric_limits<double>::max()) 
    {
      if (std::abs(startTimeRange.second - startTimeRange.first) >2*maxOffset) {
        throw EdgeDescriptionException("EdgeDescription::fixStartTimeRange: "
          "Tried to fix startTimeRange but the range is larger than the "
          "offset.");
      }
    } else {
      throw EdgeDescriptionException("EdgeDescription::fixStartTimeRange: "
        "Neither end of the startTimeRange is defined.");
    }
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

  std::string toString() const {
    std::string rString = source + " " + edgeId + " " + target + " " +
      boost::lexical_cast<std::string>(startTimeRange.first) + " " +
      boost::lexical_cast<std::string>(startTimeRange.second) + " " +
      boost::lexical_cast<std::string>(endTimeRange.first) + " " +
      boost::lexical_cast<std::string>(endTimeRange.second);
    return rString;
  }

  std::string getSource() const { return source; }
  std::string getEdgeId() const { return edgeId; }
  std::string getTarget() const { return target; }

  /**
   * Returns true if the tuple satisifies the constraints laid out by 
   * this edge description.
   */
  bool satisfies(TupleType const& tuple, double startTime) const {
    #ifdef DEBUG
    printf("EdgeDescription::satisfies tuple: %s startTime: %f\n",
      sam::toString(tuple).c_str(), startTime);
    #endif
    if (!satisfiesTimeConstraints(tuple, startTime)) {
      return false;
    }
    return true;
  }

  bool satisfiesTimeConstraints(TupleType const& tuple, double startTime) const
  {
    double edgeActualStartTime = std::get<time>(tuple);
    double edgeActualEndTime = edgeActualStartTime +
      std::get<duration>(tuple);
    double constraintStartTime_beg = startTimeRange.first + startTime; 
    double constraintStartTime_end = startTimeRange.second + startTime;
    double constraintEndTime_beg = endTimeRange.first + startTime;
    double constraintEndTime_end = endTimeRange.second + startTime;
    
    #ifdef DEBUG
    printf("EdgeDescription::satisfiesTimeConstraints tuple %s "
      "startTime %f "
      "edgeActualStartTime %f edgeActualEndTime %f "
      "startTimeRange %f %f endTimeRange %f %f "
      "constraintStartTime %f %f constraintEndTime %f %f\n",
      sam::toString(tuple).c_str(), startTime,
      edgeActualStartTime, edgeActualEndTime, 
      startTimeRange.first, startTimeRange.second,
      endTimeRange.first, endTimeRange.second,
      constraintStartTime_beg, constraintStartTime_end,
      constraintEndTime_beg, constraintEndTime_end);
    #endif

    if (edgeActualStartTime >= constraintStartTime_beg &&
        edgeActualStartTime <= constraintStartTime_end &&
        edgeActualEndTime >= constraintEndTime_beg &&
        edgeActualEndTime <= constraintEndTime_end)
    {
      #ifdef DEBUG
      printf("EdgeDescription::satisfiesTimeConstraints returning true\n");
      #endif
      return true;
    }

    #ifdef DEBUG
    printf("EdgeDescription::satisfiesTimeConstraints returning false\n");
    #endif
    return false;
  }
};





}

#endif
