#define BOOST_TEST_MAIN TestVastNetflow
#include <boost/test/unit_test.hpp>
#include <sam/tuples/NetflowV5.hpp>
#include <sam/tuples/Tuplizer.hpp>

using namespace sam;
using namespace sam::netflowv5;

typedef NetflowV5 TupleType;


void checkCommon(TupleType const& netflow) 
{
  BOOST_CHECK_EQUAL(1578588300,    std::get<UnixSecs>(netflow));
  BOOST_CHECK_EQUAL(24626000,      std::get<UnixNsecs>(netflow));
  BOOST_CHECK_EQUAL(3739416520,    std::get<SysUptime>(netflow));
  BOOST_CHECK_EQUAL("192.168.0.1", std::get<Exaddr>(netflow));
  BOOST_CHECK_EQUAL(1,             std::get<Dpkts>(netflow));
  BOOST_CHECK_EQUAL(40,            std::get<Doctets>(netflow));
  BOOST_CHECK_EQUAL(3739180654,    std::get<First1>(netflow));
  BOOST_CHECK_EQUAL(3739180654,    std::get<Last1>(netflow));
  BOOST_CHECK_EQUAL(1,             std::get<EngineType>(netflow));
  BOOST_CHECK_EQUAL(2,             std::get<EngineId>(netflow));
  BOOST_CHECK_EQUAL("192.168.0.1", std::get<SourceIp>(netflow));
  BOOST_CHECK_EQUAL("192.168.0.3", std::get<DestIp>(netflow));
  BOOST_CHECK_EQUAL("0.0.0.0",     std::get<NextHop>(netflow));
  BOOST_CHECK_EQUAL(2305,          std::get<SnmpInput>(netflow));
  BOOST_CHECK_EQUAL(2305,          std::get<SnmpOutput>(netflow));
  BOOST_CHECK_EQUAL(61811,         std::get<SourcePort>(netflow));
  BOOST_CHECK_EQUAL(80,            std::get<DestPort>(netflow));
  BOOST_CHECK_EQUAL(6,             std::get<Protocol>(netflow));
  BOOST_CHECK_EQUAL(0,             std::get<Tos>(netflow));
  BOOST_CHECK_EQUAL(20,            std::get<TcpFlags>(netflow));
  BOOST_CHECK_EQUAL(0,             std::get<SourceMask>(netflow));
  BOOST_CHECK_EQUAL(0,             std::get<DestMask>(netflow));
  BOOST_CHECK_EQUAL(0,             std::get<SourceAS>(netflow));
  BOOST_CHECK_EQUAL(0,             std::get<DestAS>(netflow));
  
}

BOOST_AUTO_TEST_CASE( test_makeNetflow )
{
  std::string s = "1578588300,24626000,3739416520,192.168.0.1,1,40,"
    "3739180654,3739180654,1,2,192.168.0.1,192.168.0.3,0.0.0.0,2305,2305,"
    "61811,80,6,0,20,0,0,0,0";

  NetflowV5 netflow = makeNetflowV5(s);
   
  checkCommon(netflow);

}

BOOST_AUTO_TEST_CASE( test_tuplizer )
{
  typedef std::tuple<int> LabelType;
  typedef Edge<size_t, LabelType, TupleType> EdgeType;
  typedef TuplizerFunction<EdgeType, MakeNetflowV5> Tuplizer;
  Tuplizer tuplizer;

  std::string s = "1,1578588300,24626000,3739416520,192.168.0.1,1,40,"
    "3739180654,3739180654,1,2,192.168.0.1,192.168.0.3,0.0.0.0,2305,2305,"
    "61811,80,6,0,20,0,0,0,0";

  EdgeType edge = tuplizer(0, s);
  checkCommon(edge.tuple);
  BOOST_CHECK_EQUAL(std::get<0>(edge.label), 1);
  BOOST_CHECK_EQUAL(std::tuple_size<decltype(edge.label)>::value, 1);

}




