/**
 * Format for VHS-22 dataset. 
 * https://www.kaggle.com/datasets/h2020simargl/vhs-22-network-traffic-dataset
 */
#ifndef SAM_VHS22_HPP
#define SAM_VHS22_HPP

#include <string>
#include <tuple>
#include <iostream>
#include <boost/tokenizer.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/fusion/algorithm/iteration/for_each.hpp>
#include <zmq.hpp>

#include <sam/Util.hpp>
#include <sam/tuples/Edge.hpp>

namespace sam {

namespace vhs22_netflow {

const unsigned int xfield                                = 0; 
const unsigned int ack_nr_count                          = 1;
const unsigned int b_packet_first_q                      = 2;
const unsigned int b_packet_max                          = 3;
const unsigned int b_packet_mean                         = 4;
const unsigned int b_packet_median                       = 5;
const unsigned int b_packet_min                          = 6;
const unsigned int b_packet_std                          = 7;
const unsigned int b_packet_third_q                      = 8;
const unsigned int b_packet_total                        = 9;
const unsigned int dport                                 = 10;
const unsigned int duration                              = 11;
const unsigned int fin_nr_count                          = 12;
const unsigned int first_timestamp                       = 13;
const unsigned int flags_sum                             = 14;
const unsigned int iat_first_q                           = 15;
const unsigned int iat_max                               = 16;
const unsigned int iat_mean                              = 17;
const unsigned int iat_median                            = 18;
const unsigned int iat_min                               = 19;
const unsigned int iat_std                               = 20;
const unsigned int iat_third_q                           = 21;
const unsigned int iat_var                               = 22;
const unsigned int in_packets                            = 23;
const unsigned int ip_dst_str                            = 24;
const unsigned int ip_protocol                           = 25;
const unsigned int ip_src_str                            = 26;
const unsigned int last_timestamp                        = 27;
const unsigned int psh_nr_count                          = 28;
const unsigned int rst_nr_count                          = 29;
const unsigned int sport                                 = 30;
const unsigned int syn_nr_count                          = 31;
const unsigned int urg_nr_count                          = 32;
const unsigned int connections_from_this_host            = 33;
const unsigned int connections_to_this_host              = 34;
const unsigned int connections_rst_to_this_host          = 35;
const unsigned int connections_rst_from_this_host        = 36;
const unsigned int connections_to_this_port              = 37;
const unsigned int connections_from_this_port            = 38;
const unsigned int connections_ratio_from_this_host      = 39;
const unsigned int connections_ratio_to_this_host        = 40;
const unsigned int connections_ratio_rst_to_this_host    = 41;
const unsigned int connections_ratio_rst_from_this_host  = 42;
const unsigned int connections_ratio_to_this_port        = 43;
const unsigned int connections_ratio_from_this_port      = 44;

/**
 * We catch exceptions generated from this file and wrap them with a 
 * VHS22NetflowException to make them easier to track.
 */
class VHS22NetflowException : public std::runtime_error {
public:
  VHS22NetflowException(char const * message) : std::runtime_error(message) { }
  VHS22NetflowException(std::string  message) : std::runtime_error(message) { }
};

typedef std::tuple<
  int, // X 2                                     
  int, // ack_nr_count 3                         
  int, // b_packet_first_q 4                      
  int, // b_packet_max     5                     
  double, // b_packet_mean    6                     
  double, // b_packet_median  7                     
  int, // b_packet_min       8                   
  double, // b_packet_std     9                     
  int, // b_packet_third_q   10                   
  int, // b_packet_total     11                   
  int,  // dport            12                     
  double, // duration       13                
  int, // fin_nr_count      14                    
  double, // first_timestamp        15               
  int, // flags_sum              16                
  int, // iat_first_q            17               
  double, // iat_max                18               
  double, // iat_mean               19               
  double, // iat_median             20               
  double, // iat_min                21               
  double, // iat_std                22               
  double, // iat_third_q            23               
  double, // iat_var                24               
  int, // in_packets             25               
  std::string, // ip_dst_str             26               
  int, // ip_protocol            27               
  std::string, // ip_src_str             28               
  double, // last_timestamp         29               
  int, // psh_nr_count           30               
  int, // rst_nr_count           31               
  int, // sport                  32               
  int, // syn_nr_count           33               
  int, // urg_nr_count           34               
  int, // connections_from_this_host 35           
  int, // connections_to_this_host   36           
  int, // connections_rst_to_this_host 37         
  int, // connections_rst_from_this_host 38       
  int, // connections_to_this_port       39       
  int, // connections_from_this_port     40       
  double, // connections_ratio_from_this_host 41     
  double, // connections_ratio_to_this_host   42     
  double, // connections_ratio_rst_to_this_host 43   
  double, // connections_ratio_rst_from_this_host 44 
  double, // connections_ratio_to_this_port       45 
  double // connections_ratio_from_this_port     46 
  > VHS22Netflow;


/**
 * Converts a string that is in csv vhs22 format into a tuple.
 */
inline
VHS22Netflow makeVHS22Netflow(std::string const& s)
{

  std::stringstream ss(s);
  std::string item;
  std::getline(ss, item, ',');
  int X = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  int ack_nr_count = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  int b_packet_first_q = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  int b_packet_max = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  double b_packet_mean = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  double b_packet_median = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  int b_packet_min = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  double b_packet_std = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  int b_packet_third_q = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  int b_packet_total = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  int dport = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  double duration = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  int fin_nr_count = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  double first_timestamp = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  int flags_sum = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  int iat_first_q = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  double iat_max = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  double iat_mean = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  double iat_median = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  double iat_min = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  double iat_std = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  double iat_third_q = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  double iat_var = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  int in_packets = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  std::string ip_dst_str = item;
  std::getline(ss, item, ',');
  int ip_protocol = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  std::string ip_src_str = item;
  std::getline(ss, item, ',');
  double last_timestamp = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  int psh_nr_count = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  int rst_nr_count = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  int sport = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  int syn_nr_count = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  int urg_nr_count = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  int connections_from_this_host = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  int connections_to_this_host = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  int connections_rst_to_this_host = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  int connections_rst_from_this_host = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  int connections_to_this_port = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  int connections_from_this_port = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  double connections_ratio_from_this_host = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  double connections_ratio_to_this_host = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  double connections_ratio_rst_to_this_host = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  double connections_ratio_rst_from_this_host=boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  double connections_ratio_to_this_port = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  double connections_ratio_from_this_port = boost::lexical_cast<double>(item);

  return std::make_tuple(
     X,
     ack_nr_count,
     b_packet_first_q,
     b_packet_max,
     b_packet_mean,
     b_packet_median,
     b_packet_min,
     b_packet_std,
     b_packet_third_q,
     b_packet_total,
     dport,
     duration,
     fin_nr_count,
     first_timestamp,
     flags_sum,
     iat_first_q,
     iat_max,
     iat_mean,
     iat_median,
     iat_min,
     iat_std,
     iat_third_q,
     iat_var,
     in_packets,
     ip_dst_str,
     ip_protocol,
     ip_src_str,
     last_timestamp,
     psh_nr_count,
     rst_nr_count,
     sport,
     syn_nr_count,
     urg_nr_count,
     connections_from_this_host,
     connections_to_this_host,
     connections_rst_to_this_host,
     connections_rst_from_this_host,
     connections_to_this_port,
     connections_from_this_port,
     connections_ratio_from_this_host,
     connections_ratio_to_this_host,
     connections_ratio_rst_to_this_host,
     connections_ratio_rst_from_this_host,
     connections_ratio_to_this_port,
     connections_ratio_from_this_port);
}

class MakeVHS22Netflow
{
public:
  VHS22Netflow operator()(std::string const& s)
  {
    return makeVHS22Netflow(s);
  }

};


} // end namespace vhs22_netflow

} // end namespace sam


#endif
