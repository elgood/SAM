#define BOOST_TEST_MAIN TestUtil
#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <tuple>
#include <string>
#include <random>
#include "Util.hpp"
#include "Netflow.hpp"

using namespace sam;


BOOST_AUTO_TEST_CASE( test_subtuple )
{

  std::string netflowString1 = "1,1,1365582756.384094,2013-04-10 08:32:36," 
                         "20130410083236.384094,17,UDP,172.20.2.18," 
                         "239.255.255.250,29986,1900,0,0,0,133,0,1,0,1,0,0";

  Netflow netflow = makeNetflow(netflowString1);

  typedef std::tuple<std::string, int> OutputType;
  OutputType outTuple;

  std::index_sequence<SourceIp, SrcTotalBytes> sequence;
  outTuple = subtuple(netflow, sequence);
  

  BOOST_CHECK_EQUAL(std::get<SourceIp>(netflow), 
                    std::get<0>(outTuple));
  BOOST_CHECK_EQUAL(std::get<SrcTotalBytes>(netflow), 
                    std::get<1>(outTuple));

}

BOOST_AUTO_TEST_CASE( test_generate_key )
{
  std::string netflowString1 = "1,1,1365582756.384094,2013-04-10 08:32:36," 
                         "20130410083236.384094,17,UDP,172.20.2.18," 
                         "239.255.255.250,29986,1900,0,0,0,133,0,1,0,1,0,0";

  Netflow netflow = makeNetflow(netflowString1);

  std::string key = generateKey<ParseDate, TimeSeconds>(netflow);
  BOOST_CHECK_EQUAL(key, "2013-04-10 08:32:361365582756.384094"); 
}

BOOST_AUTO_TEST_CASE( test_calcMean )
{
  std::random_device rd;
  std::mt19937 gen(rd());

  std::vector<double> v;
  int numIter = 100000;
  double expectedMean = 5.0;
  double expectedDev  = 2.0;
  std::normal_distribution<double> dist(expectedMean, expectedDev);
  for(int i = 0; i < numIter; i++) {
    v.push_back(dist(gen));
  }

  double mean = calcMean(v);
  double dev  = calcStandardDeviation(v);
  
  BOOST_CHECK_CLOSE(mean, expectedMean, 1);
  BOOST_CHECK_CLOSE(dev, expectedDev, 1);

}
