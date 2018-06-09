#define BOOST_TEST_MAIN TestNumTriangles

/// Some tests to make sure that the function sam::numTriangles
/// behaves correctly.

#include "Util.hpp"
#include "NetflowGenerators.hpp"
#include "Netflow.hpp"
#include <boost/test/unit_test.hpp>

using namespace sam;
using namespace std::chrono;


BOOST_AUTO_TEST_CASE( test_self_edge )
{
  /// Testing that a self edge doesn't induce a triangle


  AbstractNetflowGenerator *generator = new RandomGenerator();
  std::string str = generator->generate();

  Netflow netflow = makeNetflow(0, str);
  std::vector<Netflow> netflowList;
  netflowList.push_back(netflow);
  netflowList.push_back(netflow);
  netflowList.push_back(netflow);
  netflowList.push_back(netflow);


  size_t calculatedNumTriangles = 
    sam::numTriangles<Netflow, SourceIp, DestIp, TimeSeconds,
                      DurationSeconds>(netflowList, 10);

  BOOST_CHECK_EQUAL(0, calculatedNumTriangles); 

}
