#define BOOST_TEST_MAIN TestBaseComputation
#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <vector>
#include <string>
#include <sam/BaseComputation.hpp>
#include <sam/tuples/VastNetflow.hpp>
#include <sam/Util.hpp>

using namespace sam;
using namespace sam::vast_netflow;


BOOST_AUTO_TEST_CASE( test_generate_key )
{
  std::string netflowString1 = "1365582756.384094,2013-04-10 08:32:36," 
                         "20130410083236.384094,17,UDP,172.20.2.18," 
                         "239.255.255.250,29986,1900,0,0,0,133,0,1,0,1,0,0";
  auto featureMap = std::make_shared<FeatureMap>();
  size_t nodeId = 0;
  std::string id = "blah";
  BaseComputation computation(nodeId, featureMap, id);
                                                               
  VastNetflow netflow = makeVastNetflow(netflowString1);
  std::string key = generateKey<SourceIp>(netflow);

  BOOST_CHECK_EQUAL(key.compare("172.20.2.18"), 0);
}

