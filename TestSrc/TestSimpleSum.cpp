#define BOOST_TEST_MAIN TestSimpleSum
#include <boost/test/unit_test.hpp>
#include "SimpleSum.hpp"

using namespace sam;

BOOST_AUTO_TEST_CASE( simple_sum_test )
{
  vector<size_t> keyFields;
  keyFields.push_back(6);
  size_t valueField = 14;
  size_t nodeId = 0;
  ImuxData data;
  SimpleSum<size_t> sum(10, keyFields, valueField, nodeId, data, "sum0");

  string netflowString1 = "1365582756.384094,2013-04-10 08:32:36," 
                         "20130410083236.384094,17,UDP,172.20.2.18," 
                         "239.255.255.250,29986,1900,0,0,0,133,0,1,0,1,0,0";
  string netflowString2 = "1365582756.384094,2013-04-10 08:32:36," 
                         "20130410083236.384094,17,UDP,172.20.2.18," 
                         "239.255.255.250,29986,1900,0,0,0,133,0,2,0,1,0,0";

  sum.consume(netflowString1);
  size_t total = sum.getSum("239.255.255.250");
  BOOST_CHECK_EQUAL(total, 1);

  for (int i = 0; i < 9; i++) sum.consume(netflowString1);
  total = sum.getSum("239.255.255.250");
  BOOST_CHECK_EQUAL(total, 10);


  for (int i = 0; i < 9; i++) 
  {
    sum.consume(netflowString1);
    total = sum.getSum("239.255.255.250");
    BOOST_CHECK_EQUAL(total, 10);
  }

  sum.consume(netflowString2);
  total = sum.getSum("239.255.255.250");
  BOOST_CHECK_EQUAL(total, 11);

  sum.consume(netflowString2);
  total = sum.getSum("239.255.255.250");
  BOOST_CHECK_EQUAL(total, 12);
}
