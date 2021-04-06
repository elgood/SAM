#define BOOST_TEST_MAIN TestJaccardIndex
#include <boost/test/unit_test.hpp>
#include <sam/JaccardIndex.hpp>
#include <sam/tuples/VastNetflow.hpp>
#include <sam/tuples/Edge.hpp>
#include <sam/tuples/Tuplizer.hpp>

using namespace sam;
using namespace sam::vast_netflow;
using std::string;

typedef VastNetflow TupleType;
typedef EmptyLabel LabelType;
typedef Edge<size_t, LabelType, TupleType> EdgeType;
typedef TuplizerFunction<EdgeType, MakeVastNetflow> Tuplizer;

BOOST_AUTO_TEST_CASE( jaccard_index_test )
{
  Tuplizer tuplizer;

  std::vector<size_t> keyFields;
  size_t nodeId = 0;
  auto featureMap = std::make_shared<FeatureMap>();
  JaccardIndex<size_t, EdgeType, SrcTotalBytes, DestIp>
    ji(10, nodeId, featureMap, "sum0");

  string netflowString0 = "1365582756.384094,2013-04-10 08:32:36,"
                         "20130410083236.384094,17,UDP,172.20.2.18,"
                         "239.255.255.250,29986,1900,0,0,0,133,0,0,0,1,0,0";
  string netflowString1 = "1365582756.384094,2013-04-10 08:32:36,"
                         "20130410083236.384094,17,UDP,172.20.2.18,"
                         "239.255.255.250,29986,1900,0,0,0,133,0,1,0,1,0,0";
  string netflowString2 = "1365582756.384094,2013-04-10 08:32:36,"
                         "20130410083236.384094,17,UDP,172.20.2.18,"
                         "239.255.255.250,29986,1900,0,0,0,133,0,2,0,1,0,0";
  string netflowString3 = "1365582756.384094,2013-04-10 08:32:36,"
                          "20130410083236.384094,17,UDP,172.20.2.18,"
                          "239.255.255.250,29986,1900,0,0,0,133,0,3,0,1,0,0";
  string netflowString4 = "1365582756.384094,2013-04-10 08:32:36,"
                         "20130410083236.384094,17,UDP,172.20.2.18,"
                         "239.255.255.250,29986,1900,0,0,0,133,0,4,0,1,0,0";
  string netflowString5 = "1365582756.384094,2013-04-10 08:32:36,"
                         "20130410083236.384094,17,UDP,172.20.2.18,"
                         "239.255.255.250,29986,1900,0,0,0,133,0,5,0,1,0,0";
  string netflowString6 = "1365582756.384094,2013-04-10 08:32:36,"
                          "20130410083236.384094,17,UDP,172.20.2.18,"
                          "239.255.255.250,29986,1900,0,0,0,133,0,6,0,1,0,0";
  string netflowString7 = "1365582756.384094,2013-04-10 08:32:36,"
                         "20130410083236.384094,17,UDP,172.20.2.18,"
                         "239.255.255.250,29986,1900,0,0,0,133,0,7,0,1,0,0";
  string netflowString8 = "1365582756.384094,2013-04-10 08:32:36,"
                         "20130410083236.384094,17,UDP,172.20.2.18,"
                         "239.255.255.250,29986,1900,0,0,0,133,0,8,0,1,0,0";
  string netflowString9 = "1365582756.384094,2013-04-10 08:32:36,"
                          "20130410083236.384094,17,UDP,172.20.2.18,"
                          "239.255.255.250,29986,1900,0,0,0,133,0,9,0,1,0,0";

  EdgeType edge0 = tuplizer(0, netflowString0);
  EdgeType edge1 = tuplizer(1, netflowString1);
  EdgeType edge2 = tuplizer(2, netflowString2);
  EdgeType edge3 = tuplizer(3, netflowString3);
  EdgeType edge4 = tuplizer(4, netflowString4);
  EdgeType edge5 = tuplizer(5, netflowString5);
  EdgeType edge6 = tuplizer(6, netflowString6);
  EdgeType edge7 = tuplizer(7, netflowString7);
  EdgeType edge8 = tuplizer(8, netflowString8);
  EdgeType edge9 = tuplizer(9, netflowString9);

  // For debuging purposes initially setting jaccardindex to an invalid
  // value which should never be returned. It's a probability so it should
  // always be a value between 0 and 1.
  double jaccardindex = -0.5;

  // No edges have been consumed yet;
  // Test that JaccardIndex is checking for boundry condition
  jaccardindex = ji.getJaccardIndex("239.255.255.250");

  // All edges are identical so the two sets are also identical
  for (int i = 0; i < 1; i++) ji.consume(edge0);
  jaccardindex = ji.getJaccardIndex("239.255.255.250");
  BOOST_CHECK_EQUAL(jaccardindex, 1);

  // All edges are unique so the intersection between sets will be empty.
  ji.consume(edge1);
  ji.consume(edge2);
  ji.consume(edge3);
  ji.consume(edge4);
  ji.consume(edge5);
  ji.consume(edge6);
  ji.consume(edge7);
  ji.consume(edge8);
  ji.consume(edge9);
  ji.consume(edge0);
  jaccardindex = ji.getJaccardIndex("239.255.255.250");
  BOOST_CHECK_EQUAL(jaccardindex, 0);

  // Two edges are the same and the rest are unique so the intersection
  // between sets will be 2 items.
  ji.consume(edge1);
  ji.consume(edge2);
  ji.consume(edge3);
  ji.consume(edge4);
  ji.consume(edge5);
  ji.consume(edge1);
  ji.consume(edge2);
  ji.consume(edge8);
  ji.consume(edge9);
  ji.consume(edge0);
  jaccardindex = ji.getJaccardIndex("239.255.255.250");
  BOOST_CHECK_EQUAL(jaccardindex, 0.25);

}
