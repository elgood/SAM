#define BOOST_TEST_MAIN TestKMedian
#include <boost/test/unit_test.hpp>
#include <sam/KMedian.hpp>
#include <sam/tuples/VastNetflow.hpp>
#include <sam/tuples/Edge.hpp>
#include <sam/tuples/Tuplizer.hpp>
#include <iostream>

using namespace sam;
using namespace sam::vast_netflow;
using std::string;

typedef VastNetflow TupleType;
typedef EmptyLabel LabelType;
typedef Edge<size_t, LabelType, TupleType> EdgeType;
typedef TuplizerFunction<EdgeType, MakeVastNetflow> Tuplizer; 

BOOST_AUTO_TEST_CASE( simple_median_test )
{
  Tuplizer tuplizer;

  std::vector<size_t> keyFields;
  size_t nodeId = 0;
  auto featureMap = std::make_shared<FeatureMap>();
  // TODO: Test with 
  KMedian<size_t, EdgeType, DestIp, SrcTotalBytes> 
    kMedianTester(10, 1, nodeId, featureMap, "sum0");
                                
  // Define the Netflow strings to be used for testing 
  string netflowString1 = "1365582756.384094,2013-04-10 08:32:36," 
                         "20130410083236.384094,17,UDP,172.20.2.18," 
                         "239.255.255.250,29986,1900,0,0,0,133,0,1,0,1,0,0";
  string netflowString2 = "1365582756.384094,2013-04-10 08:32:36," 
                         "20130410083236.384094,17,UDP,172.20.2.18," 
                         "239.255.255.250,29986,1900,0,0,0,133,0,2,0,1,0,0";
  string netflowString3 = "1365582756.384094,2013-04-10 08:32:36," 
                         "20130410083236.384094,17,UDP,172.20.2.18," 
                         "239.255.255.250,29986,1900,1,2,3,4,5,6,7,8,9,10";

  EdgeType edge1 = tuplizer(1, netflowString1);
  std::cout << "Edge 1: " << edge1.toString() << std::endl;
            // << "Edge Tuple: " << edge1.tuple << std::endl;
  EdgeType edge2 = tuplizer(2, netflowString2);
  std::cout << "Edge 2: " << edge2.toString() << std::endl;
  EdgeType edge3 = tuplizer(3, netflowString3);
  std::cout << "Edge 3: " << edge3.toString() << std::endl;

  // Start the k-medians testing

  // Simple test, should equal 0 since only one value is
  // being put into the window
  kMedianTester.consume(edge1);
  size_t kMedianTemp = kMedianTester.getKMedian();
  std::cout << "KMedian: " << kMedianTemp << std::endl;
  BOOST_CHECK_EQUAL(kMedianTemp, 0);

  // Fill window all the way and the median should equal the 
  //  SrcTotalBytes value
  for (int i=0; i <= 9; i++) kMedianTester.consume(edge3);
  kMedianTemp = kMedianTester.getKMedian();
  std::cout << "KMedian: " << kMedianTemp << std::endl;
  BOOST_CHECK_EQUAL(kMedianTemp, 6);
}
