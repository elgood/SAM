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

/**
 * This is a reworking of TestSimpleSum and the tests need to be redone to
 * be appropriate for Jaccard Index.
 */
BOOST_AUTO_TEST_CASE( jaccard_index_test )
{
  Tuplizer tuplizer;

  std::vector<size_t> keyFields;
  size_t nodeId = 0;
  auto featureMap = std::make_shared<FeatureMap>();
  JaccardIndex<size_t, EdgeType, SrcTotalBytes, DestIp>
    ji(10, nodeId, featureMap, "sum0");


  string netflowString1 = "1365582756.384094,2013-04-10 08:32:36,"
                         "20130410083236.384094,17,UDP,172.20.2.18,"
                         "239.255.255.250,29986,1900,0,0,0,133,0,1,0,1,0,0";
  string netflowString2 = "1365582756.384094,2013-04-10 08:32:36,"
                         "20130410083236.384094,17,UDP,172.20.2.18,"
                         "239.255.255.250,29986,1900,0,0,0,133,0,2,0,1,0,0";

  EdgeType edge1 = tuplizer(1, netflowString1);
  EdgeType edge2 = tuplizer(2, netflowString2);

  double jaccardindex = 0.5;
  std::cout << "Test-INDEX = " << jaccardindex << std::endl; // debug

  ji.consume(edge1);
  jaccardindex = ji.getJaccardIndex("239.255.255.250");
  std::cout << "Test-INDEX = " << jaccardindex << std::endl; // debug
  BOOST_CHECK_EQUAL(jaccardindex, 0.5);

  for (int i = 0; i < 9; i++) ji.consume(edge1);
  jaccardindex = ji.getJaccardIndex("239.255.255.250");
  std::cout << "Test-INDEX = " << jaccardindex << std::endl; // debug
  BOOST_CHECK_EQUAL(jaccardindex, 10);


  for (int i = 0; i < 9; i++)
  {
    ji.consume(edge1);
    jaccardindex = ji.getJaccardIndex("239.255.255.250");
    std::cout << "Test-INDEX = " << jaccardindex << std::endl; // debug
    BOOST_CHECK_EQUAL(jaccardindex, 10);
  }

  ji.consume(edge2);
  jaccardindex = ji.getJaccardIndex("239.255.255.250");
  std::cout << "Test-INDEX = " << jaccardindex << std::endl; // debug
  BOOST_CHECK_EQUAL(jaccardindex, 11);

  ji.consume(edge2);
  jaccardindex = ji.getJaccardIndex("239.255.255.250");
  std::cout << "Test-INDEX = " << jaccardindex << std::endl; // debug
  BOOST_CHECK_EQUAL(jaccardindex, 12);
}
