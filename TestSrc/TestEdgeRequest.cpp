#define BOOST_TEST_MAIN TestEdgeRequest
#include <boost/test/unit_test.hpp>
#include "EdgeRequest.hpp"
#include "Netflow.hpp"
#include "Null.hpp"

using namespace sam;

typedef EdgeRequest<Netflow, SourceIp, DestIp> EdgeRequestType;

struct F
{
  EdgeRequestType edgeRequest;
  std::string source = "192.168.0.2";
  std::string target = "192.168.0.1";
  size_t returnNode = 1;
  double startTime = 0.0;
  double stopTime = 1.0;

  F () {
    edgeRequest.setSource(source);
    edgeRequest.setTarget(target);
    edgeRequest.setReturn(returnNode);
    edgeRequest.setStartTime(startTime);
    edgeRequest.setStopTime(stopTime);
  }
};

BOOST_FIXTURE_TEST_CASE( test_edge_request_get, F )
{
  BOOST_CHECK_EQUAL(edgeRequest.getSource(), source);
  BOOST_CHECK_EQUAL(edgeRequest.getTarget(), target);
  BOOST_CHECK_EQUAL(edgeRequest.getReturn(), returnNode);
  BOOST_CHECK_EQUAL(edgeRequest.getStartTime(), startTime);
  BOOST_CHECK_EQUAL(edgeRequest.getStopTime(), stopTime);

}

BOOST_AUTO_TEST_CASE( test_empty_fields )
{
  EdgeRequestType edgeRequest;
  
  BOOST_CHECK_EQUAL(edgeRequest.getSource(), nullValue<std::string>()); 
  BOOST_CHECK_EQUAL(edgeRequest.getTarget(), nullValue<std::string>());
  BOOST_CHECK_EQUAL(edgeRequest.getReturn(), nullValue<uint32_t>());
  BOOST_CHECK_EQUAL(edgeRequest.getStartTime(), nullValue<double>());
  BOOST_CHECK_EQUAL(edgeRequest.getStopTime(), nullValue<double>());

  BOOST_CHECK(isNull(edgeRequest.getSource()));
  BOOST_CHECK(isNull(edgeRequest.getTarget()));
  BOOST_CHECK(isNull(edgeRequest.getReturn()));
  BOOST_CHECK(isNull(edgeRequest.getStartTime()));
  BOOST_CHECK(isNull(edgeRequest.getStopTime()));

}

BOOST_FIXTURE_TEST_CASE( test_copy_constructor, F )
{
  EdgeRequestType edgeRequestCopy(edgeRequest);

  BOOST_CHECK_EQUAL(edgeRequestCopy.getSource(), source);
  BOOST_CHECK_EQUAL(edgeRequestCopy.getTarget(), target);
  BOOST_CHECK_EQUAL(edgeRequestCopy.getReturn(), returnNode);
  BOOST_CHECK_EQUAL(edgeRequestCopy.getStartTime(), startTime);
  BOOST_CHECK_EQUAL(edgeRequestCopy.getStopTime(), stopTime);

}
