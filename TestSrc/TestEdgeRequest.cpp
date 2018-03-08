#define BOOST_TEST_MAIN TestEdgeRequest
#include <boost/test/unit_test.hpp>
#include "EdgeRequest.hpp"
#include "Netflow.hpp"
#include "Null.hpp"

using namespace sam;

BOOST_AUTO_TEST_CASE( test_edge_request )
{
  typedef EdgeRequest<Netflow, SourceIp, DestIp> EdgeRequestType;

  EdgeRequestType edgeRequest;

  BOOST_CHECK(isNull(edgeRequest.getTarget()));
  BOOST_CHECK(isNull(edgeRequest.getSource()));


  std::string target = "192.168.0.1";
  std::string source = "192.168.0.2";
  edgeRequest.setTarget(target);
  edgeRequest.setSource(source);

  BOOST_CHECK_EQUAL(edgeRequest.getTarget(), target);
  BOOST_CHECK_EQUAL(edgeRequest.getSource(), source);

}
