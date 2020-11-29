#define BOOST_TEST_MAIN TestNumTriangles

/// Some tests to make sure that the function sam::numTriangles
/// behaves correctly.

//#define DEBUG

#include <sam/Util.hpp>
#include <sam/tuples/VastNetflowGenerators.hpp>
#include <sam/tuples/VastNetflow.hpp>
#include <sam/tuples/Edge.hpp>
#include <sam/tuples/Tuplizer.hpp>
#include <boost/test/unit_test.hpp>
#include <map>

using namespace sam;
using namespace sam::vast_netflow;
using namespace std::chrono;
using namespace sam::numTrianglesDetails;

typedef VastNetflow TupleType;
typedef EmptyLabel LabelType;
typedef Edge<size_t, LabelType, TupleType> EdgeType;
typedef PartialTriangle<TupleType, SourceIp, DestIp, TimeSeconds, 
          DurationSeconds> PartialTriangleType;
typedef TuplizerFunction<EdgeType, MakeVastNetflow> Tuplizer;


BOOST_AUTO_TEST_CASE( test_partial_triangle )
{

  std::map<std::string, PartialTriangleType> partialMap;
  PartialTriangleType partial;
  
  std::string s1 = "0.47,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node167,node167,51482,40020,1,1,1,1,1,1,1,1,1,1";

  VastNetflow n1 = makeVastNetflow( s1);

  partial.numEdges = 1;
  partial.netflow1 = n1;

  BOOST_CHECK_EQUAL(false, partial.isExpired(.48, 1.0));
  BOOST_CHECK_EQUAL(false, partial.isExpired(.479999999, 0.01)); 
  BOOST_CHECK_EQUAL(true, partial.isExpired(.481, 0.01)); 

}

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

  VastNetflow n1 = makeVastNetflow( s1);
  VastNetflow n2 = makeVastNetflow( s2);
  VastNetflow n3 = makeVastNetflow( s3);
  VastNetflow n4 = makeVastNetflow( s4);
  VastNetflow n5 = makeVastNetflow( s5);
  VastNetflow n6 = makeVastNetflow( s6);
  VastNetflow n7 = makeVastNetflow( s7);
  VastNetflow n8 = makeVastNetflow( s8);
  VastNetflow n9 = makeVastNetflow( s9);
  std::vector<VastNetflow> netflowList;
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
    sam::numTriangles<VastNetflow, SourceIp, DestIp, TimeSeconds,
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

  VastNetflow n1 = makeVastNetflow( s1);
  VastNetflow n2 = makeVastNetflow( s2);
  VastNetflow n3 = makeVastNetflow( s3);
  std::vector<VastNetflow> netflowList;
  netflowList.push_back(n1);
  netflowList.push_back(n2);
  netflowList.push_back(n3);
 
  size_t calculatedNumTriangles = 
    sam::numTriangles<VastNetflow, SourceIp, DestIp, TimeSeconds,
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

  VastNetflow n1 = makeVastNetflow( s1);
  VastNetflow n2 = makeVastNetflow( s2);
  std::vector<VastNetflow> netflowList;
  netflowList.push_back(n1);
  netflowList.push_back(n2);

  double queryTime = 10;
  double time = 0.2;
  double increment = 0.001;
  size_t id = 2;

  size_t n = 20000;
  size_t numValid = 0;
  for (size_t i = 0; i < n; i++) {
    std::string s = boost::lexical_cast<std::string>(time) + 
      ",parseDate,dateTimeStr,ipLayerProtocol,"
      "ipLayerProtocolCode,node3,node1,51482,40020,1,1,1,1,1,1,1,1,1,1";
    if (time <= queryTime) {
      numValid++;
      DEBUG_PRINT("valid tuple %s\n", s.c_str());
    } else {
      DEBUG_PRINT("Invalid tuple %s\n", s.c_str());
    }
    time += increment; 
    VastNetflow n = makeVastNetflow( s);
    id++;
    netflowList.push_back(n);
  }
 
  size_t calculatedNumTriangles = 
    sam::numTriangles<VastNetflow, SourceIp, DestIp, TimeSeconds,
                      DurationSeconds>(netflowList, queryTime);

  BOOST_CHECK_EQUAL(numValid, calculatedNumTriangles); 

}

BOOST_AUTO_TEST_CASE( test_counting_again )
{
  /// Creating a bunch of triangles and counting them

  std::string s1 = "0.0,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node1,node2,51482,40020,1,1,1,1,1,1,1,1,1,1";
  std::string s3 = "0.9,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node3,node1,51482,40020,1,1,1,1,1,1,1,1,1,1";

  double queryTime = 10;
  size_t n = 701;
  VastNetflow n1 = makeVastNetflow( s1);
  VastNetflow n3 = makeVastNetflow( s3);
  double timeThirdEdge = std::get<TimeSeconds>(n3);
  std::vector<VastNetflow> netflowList;
  netflowList.push_back(n1);
  netflowList.push_back(n3);

  double time = 0.2;
  double increment = 0.001;
  size_t id = 1;

  size_t numValid = 0;
  for (size_t i = 0; i < n; i++) {
    if (time <= timeThirdEdge) {
      numValid++;
    }
    std::string s = boost::lexical_cast<std::string>(time) + 
      ",parseDate,dateTimeStr,ipLayerProtocol,"
      "ipLayerProtocolCode,node2,node3,51482,40020,1,1,1,1,1,1,1,1,1,1";
    time += increment; 
    VastNetflow n = makeVastNetflow(s);
    id++;
    netflowList.push_back(n);
  }
 
  size_t calculatedNumTriangles = 
    sam::numTriangles<VastNetflow, SourceIp, DestIp, TimeSeconds,
                      DurationSeconds>(netflowList, queryTime);

  BOOST_CHECK_EQUAL(numValid, calculatedNumTriangles); 

}

BOOST_AUTO_TEST_CASE( test_specific_example )
{
  /// Testing specific example that failed
  std::string s1 = "9.8399999999998347,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node153,node111,38633,27283,1,1,1,1,1,1,1,1,1,1";
  std::string s2 = "19.100000000000186,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node111,node639,48690,30535,1,1,1,1,1,1,1,1,1,1";
  std::string s3 = "19.680000000000277,parseDate,dateTimeStr,ipLayerProtocol,"
    "ipLayerProtocolCode,node639,node153,30162,31196,1,1,1,1,1,1,1,1,1,1";

  VastNetflow n1 = makeVastNetflow( s1);
  VastNetflow n2 = makeVastNetflow( s2);
  VastNetflow n3 = makeVastNetflow( s3);
  std::vector<VastNetflow> netflowList;
  netflowList.push_back(n1);
  netflowList.push_back(n2);
  netflowList.push_back(n3);

  size_t calculatedNumTriangles = 
    sam::numTriangles<VastNetflow, SourceIp, DestIp, TimeSeconds,
                      DurationSeconds>(netflowList, 10);

  BOOST_CHECK_EQUAL(1, calculatedNumTriangles); 

}

