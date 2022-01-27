#define BOOST_TEST_MAIN TestSimpleSum
#include <boost/test/unit_test.hpp>
#include <sam/CountDistinct.hpp>
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

BOOST_AUTO_TEST_CASE( count_distinct_test )
{
  Tuplizer tuplizer;

  std::vector<size_t> keyFields;
  size_t nodeId = 0;
  auto featureMap = std::make_shared<FeatureMap>();


  // define 4 sample netflows, differ in SrcTotalBytes  field
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

  EdgeType edge1 = tuplizer(1, netflowString1);
  EdgeType edge2 = tuplizer(2, netflowString2);
  EdgeType edge3 = tuplizer(3, netflowString3);
  EdgeType edge4 = tuplizer(4, netflowString4);

  // first test object, larger window size
  CountDistinct<size_t, EdgeType, SrcTotalBytes, DestIp>
    distinct0(100, nodeId, featureMap, "distinct0");

  // sanity check
  distinct0.consume(edge1);
  size_t total = distinct0.getDistinctCount("239.255.255.250");
  BOOST_CHECK_EQUAL(total, 1);

  // verify unique count does not increase if the same value is seen
  for (int i = 0; i < 9; i++) distinct0.consume(edge1);
  total = distinct0.getDistinctCount("239.255.255.250");
  BOOST_CHECK_EQUAL(total, 1);


  // verify unique count does increase when new values are seen
  distinct0.consume(edge2);
  total = distinct0.getDistinctCount("239.255.255.250");
  BOOST_CHECK_EQUAL(total, 2);
  distinct0.consume(edge3);
  total = distinct0.getDistinctCount("239.255.255.250");
  BOOST_CHECK_EQUAL(total, 3);
  distinct0.consume(edge4);
  total = distinct0.getDistinctCount("239.255.255.250");
  BOOST_CHECK_EQUAL(total, 4);

  // verify again that unique count does not increase
  distinct0.consume(edge1);
  distinct0.consume(edge2);
  distinct0.consume(edge3);
  distinct0.consume(edge4);
  total = distinct0.getDistinctCount("239.255.255.250");
  BOOST_CHECK_EQUAL(total, 4);


  // second test object, smaller window size (must be multiple of 5 still)
  CountDistinct<size_t, EdgeType, SrcTotalBytes, DestIp>
    distinct1(5, nodeId, featureMap, "distinct1");

  // basic sanity checks, should increment as we add uniques
  distinct1.consume(edge1);
  total = distinct1.getDistinctCount("239.255.255.250");
  BOOST_CHECK_EQUAL(total, 1);

  distinct1.consume(edge2);
  total = distinct1.getDistinctCount("239.255.255.250");
  BOOST_CHECK_EQUAL(total, 2);

  distinct1.consume(edge3);
  total = distinct1.getDistinctCount("239.255.255.250");
  BOOST_CHECK_EQUAL(total, 3);

  distinct1.consume(edge4);
  total = distinct1.getDistinctCount("239.255.255.250");
  BOOST_CHECK_EQUAL(total, 4);

}
