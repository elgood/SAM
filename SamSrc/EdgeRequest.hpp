#ifndef SAM_EDGE_REQUEST
#define SAM_EDGE_REQUEST

#include "NetflowEdgeRequest.pb.h"
#include "Netflow.hpp"

namespace sam {

template <typename TupleType, size_t source, size_t target>
class EdgeRequest
{
public:
  typedef typename std::tuple_element<source, TupleType>::type SourceType;
  typedef typename std::tuple_element<target, TupleType>::type TargetType;

  void setTarget(TargetType trg) {

  }

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
      
};


}

#endif
