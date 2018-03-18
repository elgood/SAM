#ifndef SAM_EDGE_REQUEST
#define SAM_EDGE_REQUEST

#include "NetflowEdgeRequest.pb.h"
#include "Netflow.hpp"
#include "Null.hpp"
#include "Util.hpp"
#include <stdexcept>

namespace sam {

class NetflowEdgeRequestException : public std::runtime_error {
public:
  NetflowEdgeRequestException(char const * message) : 
    std::runtime_error(message) {}
  NetflowEdgeRequestException(std::string message) : 
    std::runtime_error(message) {}
};

template <typename TupleType, size_t source, size_t target>
class EdgeRequest
{
public:
  typedef typename std::tuple_element<source, TupleType>::type SourceType;
  typedef typename std::tuple_element<target, TupleType>::type TargetType;

  void setTarget(TargetType trg) {}

  void setSource(SourceType src) {}

  TargetType getTarget() const { return nullValue<TargetType>(); }

  SourceType getSource() const { return nullValue<SourceType>(); }

  zmq::message_t toZmqMessage() const { return emptyZmqMessage(); }
};

template <>
class EdgeRequest<Netflow, SourceIp, DestIp>
{
public:
  typedef typename std::tuple_element<SourceIp, Netflow>::type SourceType;
  typedef typename std::tuple_element<DestIp, Netflow>::type TargetType;
  
private:
  NetflowEdgeRequest request;
public:
  EdgeRequest() {
    
  }

  EdgeRequest(std::string str) {
    request.ParseFromString(str);
  }

  void setTarget(TargetType target) {
    request.set_destip(target);
  }

  void setSource(SourceType source) {
    request.set_sourceip(source);
  }

  TargetType getTarget() const {
    return request.destip();
  }

  TargetType getSource() const {
    return request.sourceip();
  }

  /**
   * Gets the node id of where this edge should be returned.
   */
  int getReturn() const {
    return request.returnnode();
  }

  void setReturn(int id) { 
    request.set_returnnode(id);
  }

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

  std::string toString() const
  {
    std::string rString = "Source: " + getSource() + 
                          "Target: " + getTarget();
    return rString;
  }

};


}

#endif
