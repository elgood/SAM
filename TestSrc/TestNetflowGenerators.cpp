#define BOOST_TEST_MAIN TestNetflowGenerators
#include <boost/test/unit_test.hpp>
#include <boost/tokenizer.hpp>
#include <boost/lexical_cast.hpp>
#include <stdexcept>
#include <iostream>
#include <string>
#include <map>
#include <vector>
#include "NetflowGenerators.hpp"
#include "Netflow.hpp"

using namespace sam;

/**
 * Converts a netflow string into a vetor of tokens.
 */
std::vector<std::string> convertToTokens(std::string netflowString) {
  boost::char_separator<char> sep(",");
  boost::tokenizer<boost::char_separator<char>> tokenizer(netflowString, sep);
  std::vector<std::string> v;
  for ( std::string t : tokenizer ) {
    v.push_back(t);
  }
  return v;
}

/**
 * Tests the utility function of generating random ips.
 */ 
BOOST_AUTO_TEST_CASE( test_generate_random_ip )
{ 
  for(int i = 0; i < 100000; i++) {
    std::string str = generateRandomIp();
    boost::char_separator<char> sep(".");
    boost::tokenizer<boost::char_separator<char>> tokenizer(str, sep);
    for( std::string t : tokenizer) {
      int intValue = boost::lexical_cast<int>(t);       
      BOOST_CHECK(intValue < 256);
      BOOST_CHECK(intValue >= 0);
    }
  }
}

/**
 * Since netflows are defined as tuples, interacting with them requires
 * metaprogramming.  checkTokens takes a tuple and vector and recursively
 * checks that each field in the tuple equals the corresponding element in
 * the vector.  This is the base case.
 */
template<int I = 0, typename... Tp>
inline typename std::enable_if<I == sizeof...(Tp), void>::type
checkTokens(std::tuple<Tp...> const&, std::vector<std::string> const&)
{}

/**
 * The recursive case for checkTokens.
 */
template<int I = 0, typename... Tp>
inline typename std::enable_if<I < sizeof...(Tp), void>::type
checkTokens(std::tuple<Tp...> const& t, std::vector<std::string> const& v)
{
  std::string s1 = v[I];
  std::string s2 = boost::lexical_cast<std::string>(std::get<I>(t));
  BOOST_CHECK_EQUAL(s1.compare(boost::lexical_cast<std::string>(s2)), 0);
}


BOOST_AUTO_TEST_CASE( test_netflow_conversion )
{
  std::string destIp = "192.168.0.1";
  int numPorts = 1;
  UniformDestPort generator(destIp, numPorts);
  for(int i = 0; i < 10000; i++) {
    std::string str = generator.generate();
    // makeNetflow(i, str) will add two fields, so we add them manually
    // to create the vector against which everything is checked.
    std::string addedMissing = boost::lexical_cast<std::string>(i) + "," + 
          boost::lexical_cast<std::string>(DEFAULT_LABEL) + "," + str;
    std::vector<std::string> v = convertToTokens(addedMissing);

    // We'll use i as the SamGeneratedId.
    Netflow n = makeNetflow(i, str);

    checkTokens(n, v);

  }
}

/**
 * Tests the UniformDestPort netflow generator. 
 */
BOOST_AUTO_TEST_CASE( test_uniform_dest_port )
{
  std::string destIp = "192.168.0.1";
  int numPorts = 1;
  UniformDestPort generator1(destIp, numPorts);
  int numIters = 30000;
  std::map<int, int> portCounts;

  // Generate numIters netflows. 
  for(int i = 0; i < numIters; i++) {
    std::string netflowStr = generator1.generate();

    // use i as the SamGeneratedId
    Netflow netflow = makeNetflow(i, netflowStr);
    //std::vector<std::string> v = convertToTokens(netflow);

    // In all cases the destIp should the same.
    std::string netflowDestIp = std::get<DestIp>(netflow);
    BOOST_CHECK_EQUAL(netflowDestIp.compare(destIp), 0);
    
    int destPort = std::get<DestPort>(netflow);
    portCounts[destPort]++;
  }

  for ( auto p : portCounts ) {
    BOOST_CHECK_EQUAL( static_cast<double>(p.second) / numIters,
                       static_cast<double>(1) / numPorts);
  }

  for(int j = 0; j < 10; j++) {
    numPorts = 3;
    UniformDestPort generator2(destIp, numPorts);
    numIters = 30000;
    portCounts.clear();
    for(int i = 0; i < numIters; i++) {
      std::string netflowStr = generator2.generate();
      Netflow netflow = makeNetflow(i, netflowStr);
      BOOST_CHECK_EQUAL(std::get<DestIp>(netflow).compare(destIp), 0);
      portCounts[std::get<DestPort>(netflow)]++;
    }

    
    for ( auto p : portCounts ) {
      BOOST_CHECK_EQUAL( static_cast<double>(p.second) / numIters,
                         static_cast<double>(1) / numPorts);
    }
  }
}

/**
 * Tests the OnePairSizeDist netflow generator.
 */
BOOST_AUTO_TEST_CASE( test_one_pair_size_dist )
{
  std::string destIp   = "192.168.0.1";
  std::string sourceIp = "192.186.0.2";
  double meanDestFlowSize   = 100.0;
  double meanSourceFlowSize = 50.0;
  double devDestFlowSize    = 2.0;
  double devSourceFlowSize  = 3.0;

  OnePairSizeDist generator(destIp, sourceIp, 
                               meanDestFlowSize, meanSourceFlowSize,
                               devDestFlowSize, devSourceFlowSize);

  int numIter = 100000;
  std::vector<double> destFlowSizes;
  std::vector<double> sourceFlowSizes;
  for(int i = 0; i < numIter; i++) 
  {
    std::string netflowString = generator.generate();
    Netflow netflow = makeNetflow(i, netflowString); 
    destFlowSizes.push_back(std::get<DestPayloadBytes>(netflow)); 
    sourceFlowSizes.push_back(std::get<SrcPayloadBytes>(netflow));
  }

  double m1 = calcMean(destFlowSizes);
  double m2 = calcMean(sourceFlowSizes);
  double d1 = calcStandardDeviation(destFlowSizes);
  double d2 = calcStandardDeviation(sourceFlowSizes);

  BOOST_CHECK_CLOSE(m1, meanDestFlowSize, 5);
  BOOST_CHECK_CLOSE(m2, meanSourceFlowSize, 5);
  BOOST_CHECK_CLOSE(d1, devDestFlowSize, 5);
  BOOST_CHECK_CLOSE(d2, devSourceFlowSize, 5);

}

