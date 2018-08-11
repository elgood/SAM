#ifndef SAM_EDGE_REQUEST
#define SAM_EDGE_REQUEST

#include "proto/NetflowEdgeRequest.pb.h"
#include "Netflow.hpp"
#include "Null.hpp"
#include "Util.hpp"
#include "ZeroMQUtil.hpp"
#include <stdexcept>

namespace sam {

class NetflowEdgeRequestException : public std::runtime_error {
public:
  NetflowEdgeRequestException(char const * message) : 
    std::runtime_error(message) {}
  NetflowEdgeRequestException(std::string message) : 
    std::runtime_error(message) {}
};

/**
 * This class doesn't really function.  The real EdgeRequests are 
 * defined in specializations.
 */
template <typename TupleType, size_t source, size_t target>
class EdgeRequest
{
public:
  typedef typename std::tuple_element<source, TupleType>::type SourceType;
  typedef typename std::tuple_element<target, TupleType>::type TargetType;

  void setTarget(TargetType trg) {}

  void setSource(SourceType src) {}

  double setStartTime(double startTime) { }

  TargetType getTarget() const { return nullValue<TargetType>(); }

  SourceType getSource() const { return nullValue<SourceType>(); }

  double getStartTime() const { return nullValue<double>(); }

  zmq::message_t toZmqMessage() const { return emptyZmqMessage(); }

  bool isExpired(double currentTime) const { return true; }
};


/**
 * EdgeRequest class for Netflows using SourceIp and DestIp as the
 * source and target, repsectively.  It uses the generated google protobuf. 
 */
template <>
class EdgeRequest<Netflow, SourceIp, DestIp>
{
public:
  typedef typename std::tuple_element<SourceIp, Netflow>::type SourceType;
  typedef typename std::tuple_element<DestIp, Netflow>::type TargetType;
  
private:
  NetflowEdgeRequest request;

public:
  /**
   * Default constructor.  All fields are set to the null value for each type.
   */
  EdgeRequest() {
    request.set_sourceip(nullValue<SourceType>());
    request.set_destip(nullValue<TargetType>());
    request.set_starttimefirst(nullValue<double>());
    request.set_starttimesecond(nullValue<double>());
    request.set_endtimefirst(nullValue<double>());
    request.set_endtimesecond(nullValue<double>());
    request.set_returnnode(nullValue<uint32_t>());
  }

  EdgeRequest(std::string str) { 
    request.ParseFromString(str); 
  }

  /////////// Set methods //////////////////
  void setTarget(TargetType target) { request.set_destip(target); }
  void setSource(SourceType source) { request.set_sourceip(source); }
  void setStartTimeFirst(double startTime) { 
    request.set_starttimefirst(startTime); 
  }
  void setStartTimeSecond(double startTime) { 
    request.set_starttimesecond(startTime); 
  }
  void setEndTimeFirst(double endTime) { request.set_endtimefirst(endTime); }
  void setEndTimeSecond(double endTime) { request.set_endtimesecond(endTime); }

  /**
   * Sets to which node any edges that fulfill this edge request should be 
   * sent to.
   */
  void setReturn(int id) { request.set_returnnode(id); }
    
  // Get Methods
  TargetType getTarget() const { return request.destip(); }
  TargetType getSource() const { return request.sourceip(); }
  double getStartTimeFirst() const { return request.starttimefirst(); }
  double getStartTimeSecond() const { return request.starttimesecond(); }
  double getEndTimeFirst() const { return request.endtimefirst(); }
  double getEndTimeSecond() const { return request.endtimesecond(); }

  /**
   * Gets the node id of where this edge should be returned.
   */
  uint32_t getReturn() const { return request.returnnode(); }
    
  /**
   * Transforms this edge request into a zmq message that can be sent
   * along a socket.
   */
  zmq::message_t toZmqMessage() const 
  {
    std::string str;
    bool b = request.SerializeToString(&str); 
    if (!b) {
      throw NetflowEdgeRequestException("Trouble serializing " 
        "NetflowEdgeRequest"); 
    }

    return fillZmqMessage(str);
  }

  std::string serialize() const
  {
    std::string str;
    bool b = request.SerializeToString(&str); 
    if (!b) {
      throw NetflowEdgeRequestException("Trouble serializing " 
        "NetflowEdgeRequest"); 
    }
    return str;

  }

  std::string toString() const
  {
    std::string rString = "Source: " + getSource() + 
      " Target: " + getTarget() + 
      " Return: " + boost::lexical_cast<std::string>(getReturn()) +
      " Start range: " + 
        boost::lexical_cast<std::string>(getStartTimeFirst()) + "," 
      + boost::lexical_cast<std::string>(getStartTimeSecond()) +
      " End range: " + boost::lexical_cast<std::string>(getEndTimeFirst()) + 
      "," + boost::lexical_cast<std::string>(getEndTimeSecond());
    return rString;
  }

  bool isExpired(double currentTime) const { 
    if (currentTime > getEndTimeSecond()) {
      return true;
    }
    return false; 
  }

};

}

#endif
