#define BOOST_TEST_MAIN TestVHS22Netflow
#include <boost/test/unit_test.hpp>
#include <sam/tuples/VHS22Netflow.hpp>
#include <sam/tuples/Tuplizer.hpp>

using namespace sam;
using namespace sam::vhs22_netflow;

typedef VHS22Netflow TupleType;


void checkCommon(TupleType const& netflow) 
{
  BOOST_CHECK_EQUAL(8435637,std::get<xfield>(netflow));
  BOOST_CHECK_EQUAL(0,std::get<ack_nr_count>(netflow));
  BOOST_CHECK_EQUAL(103,std::get<b_packet_first_q>(netflow));
  BOOST_CHECK_EQUAL(103,std::get<b_packet_max>(netflow));
  BOOST_CHECK_EQUAL(103,std::get<b_packet_mean>(netflow));
  BOOST_CHECK_EQUAL(103,std::get<b_packet_median>(netflow));
  BOOST_CHECK_EQUAL(103,std::get<b_packet_min>(netflow));
  BOOST_CHECK_EQUAL(0,std::get<b_packet_std>(netflow));
  BOOST_CHECK_EQUAL(103,std::get<b_packet_third_q>(netflow));
  BOOST_CHECK_EQUAL(103,std::get<b_packet_total>(netflow));
  BOOST_CHECK_EQUAL(55600,std::get<dport>(netflow));
  BOOST_CHECK_EQUAL(0,std::get<duration>(netflow));
  BOOST_CHECK_EQUAL(0,std::get<fin_nr_count>(netflow));
  BOOST_CHECK_EQUAL(50000000.50,std::get<first_timestamp>(netflow));
  BOOST_CHECK_EQUAL(0,std::get<flags_sum>(netflow));
  BOOST_CHECK_EQUAL(0,std::get<iat_first_q>(netflow));
  BOOST_CHECK_EQUAL(0,std::get<iat_max>(netflow));
  BOOST_CHECK_EQUAL(0,std::get<iat_mean>(netflow));
  BOOST_CHECK_EQUAL(0,std::get<iat_median>(netflow));
  BOOST_CHECK_EQUAL(0,std::get<iat_min>(netflow));
  BOOST_CHECK_EQUAL(0,std::get<iat_std>(netflow));
  BOOST_CHECK_EQUAL(0,std::get<iat_third_q>(netflow));
  BOOST_CHECK_EQUAL(0,std::get<iat_var>(netflow));
  BOOST_CHECK_EQUAL(1,std::get<in_packets>(netflow));
  BOOST_CHECK_EQUAL("227.213.154.241",std::get<DestIp>(netflow));
  BOOST_CHECK_EQUAL(17,std::get<ip_protocol>(netflow));
  BOOST_CHECK_EQUAL("241.255.142.237",std::get<SourceIp>(netflow));
  BOOST_CHECK_EQUAL(50000001.50,std::get<last_timestamp>(netflow));
  BOOST_CHECK_EQUAL(0,std::get<psh_nr_count>(netflow));
  BOOST_CHECK_EQUAL(0,std::get<rst_nr_count>(netflow));
  BOOST_CHECK_EQUAL(53,std::get<sport>(netflow));
  BOOST_CHECK_EQUAL(0,std::get<syn_nr_count>(netflow));
  BOOST_CHECK_EQUAL(0,std::get<urg_nr_count>(netflow));
  BOOST_CHECK_EQUAL(0,std::get<connections_from_this_host>(netflow));
  BOOST_CHECK_EQUAL(14,std::get<connections_to_this_host>(netflow));
  BOOST_CHECK_EQUAL(0,std::get<connections_rst_to_this_host>(netflow));
  BOOST_CHECK_EQUAL(0,std::get<connections_rst_from_this_host>(netflow));
  BOOST_CHECK_EQUAL(0,std::get<connections_to_this_port>(netflow));
  BOOST_CHECK_EQUAL(14,std::get<connections_from_this_port>(netflow));
  BOOST_CHECK_EQUAL(0,std::get<connections_ratio_from_this_host>(netflow));
  BOOST_CHECK_EQUAL(0.933333333333333,std::get<connections_ratio_to_this_host>(netflow));
  BOOST_CHECK_EQUAL(0,std::get<connections_ratio_rst_to_this_host>(netflow));
  BOOST_CHECK_EQUAL(0,std::get<connections_ratio_rst_from_this_host>(netflow));
  BOOST_CHECK_EQUAL(0,std::get<connections_ratio_to_this_port>(netflow));
  BOOST_CHECK_EQUAL(0.933333333333333,std::get<connections_ratio_from_this_port>(netflow));
  
}

BOOST_AUTO_TEST_CASE( test_makeNetflow )
{

  std::string s = "8435637,0,103,103,103,103,103,0,103,103,55600,0,0,50000000.50,"
    "0,0,0,0,0,0,0,0,0,1,227.213.154.241,17,241.255.142.237,50000001.50,"
    "0,0,53,0,0,0,14,0,0,0,14,0,0.933333333333333,0,0,0,0.933333333333333";

  VHS22Netflow netflow = makeVHS22Netflow(s);
   
  checkCommon(netflow);

}

BOOST_AUTO_TEST_CASE( test_vhs )
{
  // Testing data from vhs22
  std::string s = "6641098,4,74,98,76.0,98.0,66,13.1148767471313,74,304,728,0.001878023147583,1,1640995200,17,0,0.0012488365173339,0.0006260077352635,0.0003740787506103,0.0002551078796386,0.0004430765984579,0,1.96316875644698e-07,4,128.3.23.3,6,128.3.71.91,1640995200,1,0,111,1,0,0,0,0,0,0,0,0.0,0.0,0.0,0,0.0,0.0";

  VHS22Netflow netflow = makeVHS22Netflow(s);

}

BOOST_AUTO_TEST_CASE( test_tuplizer )
{
  typedef std::tuple<int> LabelType;
  typedef Edge<size_t, LabelType, TupleType> EdgeType;
  typedef TuplizerFunction<EdgeType, MakeVHS22Netflow> Tuplizer;
  Tuplizer tuplizer;

  std::string s = "1,8435637,0,103,103,103,103,103,0,103,103,55600,0,0,50000000.50,"
    "0,0,0,0,0,0,0,0,0,1,227.213.154.241,17,241.255.142.237,50000001.50,"
    "0,0,53,0,0,0,14,0,0,0,14,0,0.933333333333333,0,0,0,0.933333333333333";

  EdgeType edge = tuplizer(0, s);
  checkCommon(edge.tuple);
  BOOST_CHECK_EQUAL(std::get<0>(edge.label), 1);
  BOOST_CHECK_EQUAL(std::tuple_size<decltype(edge.label)>::value, 1);

}




