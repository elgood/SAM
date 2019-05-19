#define BOOST_TEST_MAIN TestEdgeRequest
#include <boost/test/unit_test.hpp>
#include <sam/EdgeRequest.hpp>
#include <sam/VastNetflow.hpp>
#include <sam/Null.hpp>

using namespace sam;

typedef EdgeRequest<VastNetflow, SourceIp, DestIp> EdgeRequestType;

struct F
{
  EdgeRequestType edgeRequest;
  std::string source = "192.168.0.2";
  std::string target = "192.168.0.1";
  size_t returnNode = 1;
  double startTimeFirst = 1.0;
  double startTimeSecond = 2.0;
  double endTimeFirst = 1.0;
  double endTimeSecond = 2.0;

  F () {
    edgeRequest.setSource(source);
    edgeRequest.setTarget(target);
    edgeRequest.setReturn(returnNode);
    edgeRequest.setStartTimeFirst(startTimeFirst);
    edgeRequest.setStartTimeSecond(startTimeSecond);
    edgeRequest.setEndTimeFirst(endTimeFirst);
    edgeRequest.setEndTimeSecond(endTimeSecond);
  }
};

BOOST_FIXTURE_TEST_CASE( test_edge_request_get, F )
{
  BOOST_CHECK_EQUAL(edgeRequest.getSource(), source);
  BOOST_CHECK_EQUAL(edgeRequest.getTarget(), target);
  BOOST_CHECK_EQUAL(edgeRequest.getReturn(), returnNode);
  BOOST_CHECK_EQUAL(edgeRequest.getStartTimeFirst(), startTimeFirst);
  BOOST_CHECK_EQUAL(edgeRequest.getStartTimeSecond(), startTimeSecond);
  BOOST_CHECK_EQUAL(edgeRequest.getEndTimeFirst(), endTimeFirst);
  BOOST_CHECK_EQUAL(edgeRequest.getEndTimeSecond(), endTimeSecond);
}

BOOST_AUTO_TEST_CASE( test_empty_fields )
{
  EdgeRequestType edgeRequest;
  
  BOOST_CHECK_EQUAL(edgeRequest.getSource(), nullValue<std::string>()); 
  BOOST_CHECK_EQUAL(edgeRequest.getTarget(), nullValue<std::string>());
  BOOST_CHECK_EQUAL(edgeRequest.getReturn(), nullValue<uint32_t>());
  BOOST_CHECK_EQUAL(edgeRequest.getStartTimeFirst(), nullValue<double>());
  BOOST_CHECK_EQUAL(edgeRequest.getStartTimeSecond(), nullValue<double>());
  BOOST_CHECK_EQUAL(edgeRequest.getEndTimeFirst(), nullValue<double>());
  BOOST_CHECK_EQUAL(edgeRequest.getEndTimeSecond(), nullValue<double>());

  BOOST_CHECK(isNull(edgeRequest.getSource()));
  BOOST_CHECK(isNull(edgeRequest.getTarget()));
  BOOST_CHECK(isNull(edgeRequest.getReturn()));
  BOOST_CHECK(isNull(edgeRequest.getStartTimeFirst()));
  BOOST_CHECK(isNull(edgeRequest.getStartTimeSecond()));
  BOOST_CHECK(isNull(edgeRequest.getEndTimeFirst()));
  BOOST_CHECK(isNull(edgeRequest.getEndTimeSecond()));

}

BOOST_FIXTURE_TEST_CASE( test_copy_constructor, F )
{
  EdgeRequestType edgeRequestCopy(edgeRequest);

  BOOST_CHECK_EQUAL(edgeRequestCopy.getSource(), source);
  BOOST_CHECK_EQUAL(edgeRequestCopy.getTarget(), target);
  BOOST_CHECK_EQUAL(edgeRequestCopy.getReturn(), returnNode);
  BOOST_CHECK_EQUAL(edgeRequestCopy.getStartTimeFirst(), startTimeFirst);
  BOOST_CHECK_EQUAL(edgeRequestCopy.getStartTimeSecond(), startTimeSecond);
  BOOST_CHECK_EQUAL(edgeRequestCopy.getEndTimeFirst(), endTimeFirst);
  BOOST_CHECK_EQUAL(edgeRequestCopy.getEndTimeSecond(), endTimeSecond);
}

BOOST_FIXTURE_TEST_CASE( test_zmq, F)
{

  zmq::message_t message = edgeRequest.toZmqMessage();
  std::string str = getStringFromZmqMessage(message);
  EdgeRequestType edgeRequest2(str);
  BOOST_CHECK_EQUAL(edgeRequest.getSource(), edgeRequest2.getSource());
  BOOST_CHECK_EQUAL(edgeRequest.getTarget(), edgeRequest2.getTarget());
  BOOST_CHECK_EQUAL(edgeRequest.getReturn(), edgeRequest2.getReturn());
  BOOST_CHECK_EQUAL(edgeRequest.getStartTimeFirst(), 
                    edgeRequest2.getStartTimeFirst());
  BOOST_CHECK_EQUAL(edgeRequest.getStartTimeSecond(), 
                    edgeRequest2.getStartTimeSecond());
  BOOST_CHECK_EQUAL(edgeRequest.getEndTimeFirst(), 
                    edgeRequest2.getEndTimeFirst());
  BOOST_CHECK_EQUAL(edgeRequest.getEndTimeSecond(), 
                    edgeRequest2.getEndTimeSecond());

}
