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
  CountDistinct<size_t, EdgeType, SrcTotalBytes, DestIp>
    distinct(10, nodeId, featureMap, "distinct0");


  string netflowString1 = "1365582756.384094,2013-04-10 08:32:36,"
                         "20130410083236.384094,17,UDP,172.20.2.18,"
                         "239.255.255.250,29986,1900,0,0,0,133,0,1,0,1,0,0";
  string netflowString2 = "1365582756.384094,2013-04-10 08:32:36,"
                         "20130410083236.384094,17,UDP,172.20.2.18,"
                         "239.255.255.250,29986,1900,0,0,0,133,0,2,0,1,0,0";

  EdgeType edge1 = tuplizer(1, netflowString1);
  EdgeType edge2 = tuplizer(2, netflowString2);


  distinct.consume(edge1);
  size_t total = distinct.getDistinctCount("239.255.255.250");
  BOOST_CHECK_EQUAL(total, 1);

  for (int i = 0; i < 9; i++) distinct.consume(edge1);
  total = distinct.getDistinctCount("239.255.255.250");
  BOOST_CHECK_EQUAL(total, 1);


  distinct.consume(edge2);
  total = distinct.getDistinctCount("239.255.255.250");
  BOOST_CHECK_EQUAL(total, 2);

  distinct.consume(edge2);
  total = distinct.getDistinctCount("239.255.255.250");
  BOOST_CHECK_EQUAL(total, 2);
}
