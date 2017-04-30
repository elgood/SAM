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

template<int I = 0, typename... Tp>
inline typename std::enable_if<I == sizeof...(Tp), void>::type
checkTokens(std::tuple<Tp...> const&, std::vector<std::string> const&)
{}

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
    std::vector<std::string> v = convertToTokens(str);
    Netflow n = makeNetflow(str);

    checkTokens(n, v);

    /*for_each(n, f); 
    for (int i = 0; i < v.size(); i++) {
      BOOST_CHECK_EQUAL(v[i].compare(n.getField(i)), 0);      
    }*/
  }
}


BOOST_AUTO_TEST_CASE( test_uniform_dest_port )
{
  std::string destIp = "192.168.0.1";
  int numPorts = 1;
  UniformDestPort generator1(destIp, numPorts);
  int numIters = 30000;
  std::map<int, int> portCounts;
  for(int i = 0; i < numIters; i++) {
    std::string netflow = generator1.generate();
    std::vector<std::string> v = convertToTokens(netflow);
    BOOST_CHECK_EQUAL(v[6].compare(destIp), 0);
    portCounts[boost::lexical_cast<int>(v[8])]++;
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
      std::string netflow = generator2.generate();
      boost::char_separator<char> sep(",");
      boost::tokenizer<boost::char_separator<char>> tokenizer(netflow, sep);
      std::vector<std::string> v;
      for ( std::string t : tokenizer ) {
        v.push_back(t);
      }
      BOOST_CHECK_EQUAL(v[6].compare(destIp), 0);
      portCounts[boost::lexical_cast<int>(v[8])]++;
    }

    for ( auto p : portCounts ) {
      BOOST_CHECK_EQUAL( static_cast<double>(p.second) / numIters,
                         static_cast<double>(1) / numPorts);
    }
  }


}

