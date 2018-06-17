#define BOOST_TEST_MAIN TestNumTriangles

/// Some tests to make sure that the function sam::numTriangles
/// behaves correctly.

//#define DEBUG

#include "Util.hpp"
#include "NetflowGenerators.hpp"
#include "Netflow.hpp"
#include <boost/test/unit_test.hpp>

using namespace sam;
using namespace std::chrono;


BOOST_AUTO_TEST_CASE( test_self_edge )
{
  /// Testing self edges creating a triangle.

  std::string s1 = "0.47000000000000025,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node167,node167,51482,40020,1,1,1,1,1,1,1,1,1,1";
  std::string s2 = "0.52000000000000024,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node167,node167,51482,40020,1,1,1,1,1,1,1,1,1,1";
  std::string s3 = "0.57000000000000028,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node167,node167,51482,40020,1,1,1,1,1,1,1,1,1,1";
  std::string s4 = "0.58000000000000029,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node167,node167,51482,40020,1,1,1,1,1,1,1,1,1,1";
  std::string s5 = "0.5900000000000003,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node167,node167,51482,40020,1,1,1,1,1,1,1,1,1,1";
  std::string s6 = "0.5900000000000003,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node167,node167,51482,40020,1,1,1,1,1,1,1,1,1,1";
  std::string s7 = "0.5900000000000003,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node167,node167,51482,40020,1,1,1,1,1,1,1,1,1,1";
  std::string s8 = "0.57000000000000028,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node167,node167,51482,40020,1,1,1,1,1,1,1,1,1,1";
  std::string s9 = "0.58000000000000029,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node167,node167,51482,40020,1,1,1,1,1,1,1,1,1,1";



  Netflow n1 = makeNetflow(0, s1);
  Netflow n2 = makeNetflow(1, s2);
  Netflow n3 = makeNetflow(2, s3);
  Netflow n4 = makeNetflow(3, s4);
  Netflow n5 = makeNetflow(4, s5);
  Netflow n6 = makeNetflow(5, s6);
  Netflow n7 = makeNetflow(6, s7);
  Netflow n8 = makeNetflow(7, s8);
  Netflow n9 = makeNetflow(8, s9);
  std::vector<Netflow> netflowList;
  netflowList.push_back(n1);
  netflowList.push_back(n2);
  netflowList.push_back(n3);
  //netflowList.push_back(n4);
  //netflowList.push_back(n5);
  //netflowList.push_back(n6);
  //netflowList.push_back(n7);
  //netflowList.push_back(n8);
  //netflowList.push_back(n9);
 
  size_t calculatedNumTriangles = 
    sam::numTriangles<Netflow, SourceIp, DestIp, TimeSeconds,
                      DurationSeconds>(netflowList, 10);

  BOOST_CHECK_EQUAL(1, calculatedNumTriangles); 

}


BOOST_AUTO_TEST_CASE( test_edge_same_time )
{
  /// Testing when two edges have the same time.  We want strictly increasing
  /// times, so there should be no results.

  std::string s1 = "0.47000000000000025,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node167,node167,51482,40020,1,1,1,1,1,1,1,1,1,1";
  std::string s2 = "0.52000000000000024,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node167,node167,51482,40020,1,1,1,1,1,1,1,1,1,1";
  std::string s3 = "0.52000000000000024,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node167,node167,51482,40020,1,1,1,1,1,1,1,1,1,1";

  Netflow n1 = makeNetflow(0, s1);
  Netflow n2 = makeNetflow(1, s2);
  Netflow n3 = makeNetflow(2, s3);
  std::vector<Netflow> netflowList;
  netflowList.push_back(n1);
  netflowList.push_back(n2);
  netflowList.push_back(n3);
 
  size_t calculatedNumTriangles = 
    sam::numTriangles<Netflow, SourceIp, DestIp, TimeSeconds,
                      DurationSeconds>(netflowList, 10);

  BOOST_CHECK_EQUAL(0, calculatedNumTriangles); 

}

BOOST_AUTO_TEST_CASE( test_counting )
{
  /// Creating a bunch of triangles and counting them

  std::string s1 = "0.0,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node1,node2,51482,40020,1,1,1,1,1,1,1,1,1,1";
  std::string s2 = "0.1,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node2,node3,51482,40020,1,1,1,1,1,1,1,1,1,1";

  Netflow n1 = makeNetflow(0, s1);
  Netflow n2 = makeNetflow(1, s2);
  std::vector<Netflow> netflowList;
  netflowList.push_back(n1);
  netflowList.push_back(n2);

  double time = 0.2;
  double increment = 0.001;
  size_t id = 2;

  size_t n = 100;
  for (size_t i = 0; i < n; i++) {
    std::string s = boost::lexical_cast<std::string>(time) + 
      ",parseDate,dateTimeStr,ipLayerProtocol,"
      "ipLayerProtocolCode,node3,node1,51482,40020,1,1,1,1,1,1,1,1,1,1";
    time += increment; 
    Netflow n = makeNetflow(id, s);
    id++;
    netflowList.push_back(n);
  }
 
  size_t calculatedNumTriangles = 
    sam::numTriangles<Netflow, SourceIp, DestIp, TimeSeconds,
                      DurationSeconds>(netflowList, 10);

  BOOST_CHECK_EQUAL(n, calculatedNumTriangles); 

}

BOOST_AUTO_TEST_CASE( test_counting_again )
{
  /// Creating a bunch of triangles and counting them

  std::string s1 = "0.0,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node1,node2,51482,40020,1,1,1,1,1,1,1,1,1,1";
  std::string s3 = "0.9,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node3,node1,51482,40020,1,1,1,1,1,1,1,1,1,1";

  size_t n = 100;
  Netflow n1 = makeNetflow(0, s1);
  Netflow n3 = makeNetflow(n, s3);
  std::vector<Netflow> netflowList;
  netflowList.push_back(n1);
  netflowList.push_back(n3);

  double time = 0.2;
  double increment = 0.001;
  size_t id = 1;

  for (size_t i = 0; i < n; i++) {
    std::string s = boost::lexical_cast<std::string>(time) + 
      ",parseDate,dateTimeStr,ipLayerProtocol,"
      "ipLayerProtocolCode,node2,node3,51482,40020,1,1,1,1,1,1,1,1,1,1";
    time += increment; 
    Netflow n = makeNetflow(id, s);
    id++;
    netflowList.push_back(n);
  }
 
  size_t calculatedNumTriangles = 
    sam::numTriangles<Netflow, SourceIp, DestIp, TimeSeconds,
                      DurationSeconds>(netflowList, 10);

  BOOST_CHECK_EQUAL(n, calculatedNumTriangles); 

}


