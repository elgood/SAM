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
const unsigned int DestIp                            = 24;
const unsigned int ip_protocol                           = 25;
const unsigned int SourceIp                            = 26;
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
  std::string, // DestIp             26               
  int, // ip_protocol            27               
  std::string, // SourceIp             28               
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
  int X;
  int ack_nr_count;
  int b_packet_first_q;
  int b_packet_max;
  double b_packet_mean; 
  double b_packet_median;
  int b_packet_min;
  double b_packet_std;
  int b_packet_third_q;
  int b_packet_total;
  int dport;
  double duration;
  int fin_nr_count;
  double first_timestamp;
  int flags_sum;
  int iat_first_q;
  double iat_max;
  double iat_mean;
  double iat_median;
  double iat_min;
  double iat_std;
  double iat_third_q;
  double iat_var;
  int in_packets;
  std::string DestIp;
  int ip_protocol;
  std::string SourceIp;
  double last_timestamp;
  int psh_nr_count;
  int rst_nr_count;
  int sport;
  int syn_nr_count;
  int urg_nr_count;
  int connections_from_this_host;
  int connections_to_this_host;
  int connections_rst_to_this_host;
  int connections_rst_from_this_host;
  int connections_to_this_port;
  int connections_from_this_port;
  double connections_ratio_from_this_host;
  double connections_ratio_to_this_host;
  double connections_ratio_rst_to_this_host;
  double connections_ratio_rst_from_this_host;
  double connections_ratio_to_this_port;

  // TODO Something is going on with this field, maybe end of line stuff
  double connections_ratio_from_this_port = 0;

  std::stringstream ss(s);
  std::string item;
  
  std::getline(ss, item, ',');
  try {
    X = boost::lexical_cast<int>(item);
  } catch (...) {
    throw VHS22NetflowException("Troubles parsing X with line: " + s);
  }

  std::getline(ss, item, ',');
  try {
    ack_nr_count = boost::lexical_cast<int>(item);
  } catch (...) {
    throw VHS22NetflowException("Troubles parsing ack_nr_count with line: " + s);
  }

  std::getline(ss, item, ',');
  try {
    b_packet_first_q = boost::lexical_cast<int>(item);
  } catch (...) {
    throw VHS22NetflowException("Troubles parsing b_packet_first_q with line: " + s);
  }

  std::getline(ss, item, ',');
  try {
    b_packet_max = boost::lexical_cast<int>(item);
  } catch (...) {
    throw VHS22NetflowException("Troubles parsing b_packet_max with line: " + s);
  }

  std::getline(ss, item, ',');
  try {
    b_packet_mean = boost::lexical_cast<double>(item);
  } catch (...) {
    throw VHS22NetflowException("Troubles parsing b_packet_max with line: " + s);
  }

  std::getline(ss, item, ',');
  try {
  b_packet_median = boost::lexical_cast<double>(item);
  } catch (...) {
    throw VHS22NetflowException("Troubles parsing b_packet_median with line: " + s);
  }

  std::getline(ss, item, ',');
  try {
  b_packet_min = boost::lexical_cast<int>(item);
  } catch (...) {
    throw VHS22NetflowException("Troubles parsing b_packet_min with line: " + s);
  }
  
  std::getline(ss, item, ',');
  try {
  b_packet_std = boost::lexical_cast<double>(item);
  } catch (...) {
    throw VHS22NetflowException("Troubles parsing b_packet_std with line: " + s);
  }
  
  std::getline(ss, item, ',');
  try {
  b_packet_third_q = boost::lexical_cast<int>(item);
  } catch (...) {
    throw VHS22NetflowException("Troubles parsing b_packet_third_q with line: " + s);
  }
  
  std::getline(ss, item, ',');
  try {
  b_packet_total = boost::lexical_cast<int>(item);
  } catch (...) {
    throw VHS22NetflowException("Troubles parsing b_packet_total with line: " + s);
  }
  
  std::getline(ss, item, ',');
  try {
  dport = boost::lexical_cast<int>(item);
  } catch (...) {
    throw VHS22NetflowException("Troubles parsing dport with line: " + s);
  }
  
  std::getline(ss, item, ',');
  try {
  duration = boost::lexical_cast<double>(item);
  } catch (...) {
    throw VHS22NetflowException("Troubles parsing duration with line: " + s);
  }
  
  std::getline(ss, item, ',');
  try {
  fin_nr_count = boost::lexical_cast<int>(item);
  } catch (...) {
    throw VHS22NetflowException("Troubles parsing fin_nr_count with line: " + s);
  }
  
  std::getline(ss, item, ',');
  try {
  first_timestamp = boost::lexical_cast<double>(item);
  } catch (...) {
    throw VHS22NetflowException("Troubles parsing first_timestamp with line: " + s);
  }
  
  std::getline(ss, item, ',');
  try {
  flags_sum = boost::lexical_cast<int>(item);
  } catch (...) {
    throw VHS22NetflowException("Troubles parsing flags_sum with line: " + s);
  }
  
  std::getline(ss, item, ',');
  try {
  iat_first_q = boost::lexical_cast<int>(item);
  } catch (...) {
    throw VHS22NetflowException("Troubles parsing iat_first_q with line: " + s);
  }
  
  std::getline(ss, item, ',');
  iat_max = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  iat_mean = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  iat_median = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  iat_min = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  iat_std = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  iat_third_q = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  iat_var = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  in_packets = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  DestIp = item;
  std::getline(ss, item, ',');
  ip_protocol = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  SourceIp = item;
  std::getline(ss, item, ',');
  last_timestamp = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  psh_nr_count = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  rst_nr_count = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  sport = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  syn_nr_count = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  urg_nr_count = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  connections_from_this_host = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  connections_to_this_host = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  connections_rst_to_this_host = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  connections_rst_from_this_host = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  connections_to_this_port = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  connections_from_this_port = boost::lexical_cast<int>(item);
  std::getline(ss, item, ',');
  connections_ratio_from_this_host = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  connections_ratio_to_this_host = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  connections_ratio_rst_to_this_host = boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  connections_ratio_rst_from_this_host=boost::lexical_cast<double>(item);
  std::getline(ss, item, ',');
  connections_ratio_to_this_port = boost::lexical_cast<double>(item);
  
  std::getline(ss, item, ',');
  try {
  connections_ratio_from_this_port = boost::lexical_cast<double>(item);
  } catch (...) {
    throw VHS22NetflowException(
      "Troubles parsing connections_ratio_from_this_port with line: " + s);
  }



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
     DestIp,
     ip_protocol,
     SourceIp,
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
