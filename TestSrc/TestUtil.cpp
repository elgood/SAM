#define BOOST_TEST_MAIN TestUtil
#include <boost/test/unit_test.hpp>
#include <boost/tokenizer.hpp>
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

BOOST_AUTO_TEST_CASE( test_toString_tuple ) {
  // Create a simple tuple
  double d = 1.0;
  int i = 8;
  std::string s = "blah";
  
  auto tuple = std::make_tuple(d, i, s);
  std::string result = toString(tuple);

  // Make sure it doesn't end with a comma
  BOOST_CHECK(result[result.size() - 1] != ',');

  // Tokenize the stringified tuple by commas
  boost::char_separator<char> sep(",");
  boost::tokenizer<boost::char_separator<char>> tok(result, sep);

  // Iterate over the tokens and make sure they match the original tokens.
  int n = 0;
  BOOST_FOREACH(std::string const &t, tok)
  {
    switch (n) {
    case 0: BOOST_CHECK_EQUAL(boost::lexical_cast<std::string>(d), t); break;
    case 1: BOOST_CHECK_EQUAL(boost::lexical_cast<std::string>(i), t); break;
    case 2: BOOST_CHECK_EQUAL(s, t); break;
    default: BOOST_CHECK( false ); //Shouldn't get here if everything good.
    }
    n++;
  }
}

BOOST_AUTO_TEST_CASE( test_makeTuple_to_toString)
{
  std::string netflowString = "1,1,1365663544.4683361,2013-04-11" 
    " 06:59:04,20130411065904.468336,6,TCP,172.20.1.93,10.0.0.10,10582" 
    ",80,0,0,16,184,73140,2588,76064,40,54,0";

  Netflow netflow = makeNetflow(netflowString);
  
  std::string stringAgain = toString(netflow);
  BOOST_CHECK_EQUAL(netflowString, stringAgain); 
}

BOOST_AUTO_TEST_CASE( test_empty_zmq_message)
{
  zmq::message_t message = emptyZmqMessage();
  char* buff = static_cast<char*>(message.data());
  std::cout << buff << std::endl;
  BOOST_CHECK_EQUAL(buff, "");

  BOOST_CHECK(isTerminateMessage(message));

}
