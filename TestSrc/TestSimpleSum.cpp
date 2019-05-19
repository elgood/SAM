#define BOOST_TEST_MAIN TestSimpleSum
#include <boost/test/unit_test.hpp>
#include <sam/SimpleSum.hpp>
#include <sam/VastNetflow.hpp>

using namespace sam;
using std::string;

BOOST_AUTO_TEST_CASE( simple_sum_test )
{
  std::vector<size_t> keyFields;
  size_t nodeId = 0;
  auto featureMap = std::make_shared<FeatureMap>();
  SimpleSum<size_t, VastNetflow, SrcTotalBytes, DestIp> 
    sum(10, nodeId, featureMap, "sum0");
                                

  string netflowString1 = "1365582756.384094,2013-04-10 08:32:36," 
                         "20130410083236.384094,17,UDP,172.20.2.18," 
                         "239.255.255.250,29986,1900,0,0,0,133,0,1,0,1,0,0";
  string netflowString2 = "1365582756.384094,2013-04-10 08:32:36," 
                         "20130410083236.384094,17,UDP,172.20.2.18," 
                         "239.255.255.250,29986,1900,0,0,0,133,0,2,0,1,0,0";
  VastNetflow n1 = makeNetflow(1,netflowString1);
  VastNetflow n2 = makeNetflow(2,netflowString2);

  sum.consume(n1);
  size_t total = sum.getSum("239.255.255.250");
  BOOST_CHECK_EQUAL(total, 1);

  for (int i = 0; i < 9; i++) sum.consume(n1);
  total = sum.getSum("239.255.255.250");
  BOOST_CHECK_EQUAL(total, 10);


  for (int i = 0; i < 9; i++) 
  {
    sum.consume(n1);
    total = sum.getSum("239.255.255.250");
    BOOST_CHECK_EQUAL(total, 10);
  }

  sum.consume(n2);
  total = sum.getSum("239.255.255.250");
  BOOST_CHECK_EQUAL(total, 11);

  sum.consume(n2);
  total = sum.getSum("239.255.255.250");
  BOOST_CHECK_EQUAL(total, 12);
}
