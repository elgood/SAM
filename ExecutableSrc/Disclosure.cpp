/*
 * Servers.cpp
 * This does the server query as described in Disclosure.
 *
 *  Created on: March 15, 2017
 *      Author: elgood
 */

#include <string>
#include <vector>
#include <stdlib.h>
#include <iostream>
#include <chrono>

#include <boost/program_options.hpp>

#include "ReadSocket.h"
#include "ZeroMQPushPull.h"
#include "TopK.hpp"
#include "Expression.hpp"
#include "TupleExpression.hpp"
#include "Filter.hpp"
#include "ExponentialHistogramSum.hpp"
#include "ExponentialHistogramVariance.hpp"
#include "Netflow.hpp"
#include "TransformProducer.hpp"
#include "Project.hpp"
#include "CollapsedConsumer.hpp"

#define DEBUG 1

using std::string;
using std::vector;
using std::cout;
using std::endl;

namespace po = boost::program_options;

using namespace sam;
using namespace std::chrono;

int main(int argc, char** argv) {

#ifdef DEBUG
  cout << "DEBUG: At the beginning of main" << endl;
#endif

  // The ip to read the nc data from.
  string ip;

  // The port to read the nc data from.
  int ncPort;

  // The number of nodes in the cluster
  int numNodes;

  // The node id of this node
  int nodeId;

  // The prefix to the nodes
  string prefix;

  // The starting port number
  int startingPort;

  // The high-water mark
  long hwm;

  // The length of the input queue
  int queueLength;

  // The total number of elements in a sliding window
  int N;

  // The number of elements in a dormant or active window
  int b;

  // The number of elements to keep track of
  int k;

  int nop; //not used

  time_t timestamp_sec1, timestamp_sec2;

  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "help message")
    ("ip", po::value<string>(&ip)->default_value("localhost"), 
      "The ip to receive the data from nc")
    ("ncPort", po::value<int>(&ncPort)->default_value(9999), 
      "The port to receive the data from nc")
    ("numNodes", po::value<int>(&numNodes)->default_value(1), 
      "The number of nodes involved in the computation")
    ("nodeId", po::value<int>(&nodeId)->default_value(0), 
      "The node id of this node")
    ("prefix", po::value<string>(&prefix)->default_value("node"), 
      "The prefix common to all nodes")
    ("startingPort", po::value<int>(&startingPort)->default_value(10000), 
      "The starting port for the zeromq communications")
    ("hwm", po::value<long>(&hwm)->default_value(10000), 
      "The high water mark (how many items can queue up before we start "
      "dropping)")
    ("queueLength", po::value<int>(&queueLength)->default_value(10000),
      "We fill a queue before sending things in parallel to all consumers."
      "  This controls the size of that queue.")
    ("N", po::value<int>(&N)->default_value(10000),
      "The total number of elements in a sliding window")
    ("b", po::value<int>(&b)->default_value(1000),
      "The number of elements per block (active or dynamic window)")
    ("nop", po::value<int>(&nop)->default_value(1),
      "The number of simultaneous operators")
  ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 1;
  }

#ifdef DEBUG
  cout << "Options" << endl;
  cout << "numNodes " << numNodes << endl;
  cout << "nodeId " << nodeId << endl;
#endif


  ReadSocket receiver(ip, ncPort);
#ifdef DEBUG
  cout << "DEBUG: main created receiver " << endl;
#endif

  vector<string> hostnames(numNodes);
  vector<int> ports(numNodes);

  if (numNodes == 1) {
    hostnames[0] = "127.0.0.1";
    ports[0] = startingPort;
  } else {
    for (int i = 0; i < numNodes; i++) {
      hostnames[i] = prefix + boost::lexical_cast<string>(i);
      ports[i] = (startingPort + i);  
    }
  }

  auto consumer = std::make_shared<ZeroMQPushPull>(queueLength,
                               numNodes, 
                               nodeId, 
                               hostnames, 
                               ports, 
                               hwm);

#ifdef DEBUG
  cout << "DEBUG: main created consumer " << endl;
#endif

  receiver.registerConsumer(consumer);

  FeatureMap featureMap;

  string identifier = "top2";
  k = 2;
  auto topk = std::make_shared<TopK<size_t, Netflow, DestPort, 
                              DestIp>>
                              (N, b, k, nodeId, featureMap, identifier);
                               
  consumer->registerConsumer(topk); 

  // Five tokens for the 
  // First function token
  int index1 = 0;
  auto function1 = [&index1](Feature const * feature)->double {
    auto topKFeature = static_cast<TopKFeature const *>(feature);
    return topKFeature->getFrequencies()[index1];    
  };
  auto funcToken1 = std::make_shared<FuncToken<Netflow>>(featureMap, 
                                                        function1,
                                                        identifier);

  // Addition token
  auto addOper = std::make_shared<AddOperator<Netflow>>(featureMap);

  // Second function token
  int index2 = 1;
  auto function2 = [&index2](Feature const * feature)->double {
    auto topKFeature = static_cast<TopKFeature const *>(feature);
    return topKFeature->getFrequencies()[index2];    
  };
  auto funcToken2 = std::make_shared<FuncToken<Netflow>>(featureMap, 
                                                         function2,
                                                         identifier);

  // Lessthan token
  auto lessThanToken = std::make_shared<LessThanOperator<Netflow>>(
                        featureMap);
  
  // Number token
  auto numberToken = std::make_shared<NumberToken<Netflow>>(featureMap, 
                                                            0.9);

  std::list<std::shared_ptr<ExpressionToken<Netflow>>> infixList;
  infixList.push_back(funcToken1);
  infixList.push_back(addOper);
  infixList.push_back(funcToken2);
  infixList.push_back(lessThanToken);
  infixList.push_back(numberToken);

  Expression<Netflow> filterExpression(infixList);
    
  auto filter = std::make_shared<Filter<Netflow, DestIp>>(
    filterExpression, nodeId, featureMap, "servers", queueLength);

  consumer->registerConsumer(filter);

  identifier = "serverSumIncomingFlowSize";
  auto sumIncoming = std::make_shared<ExponentialHistogramSum<size_t, Netflow,
                                                 SrcTotalBytes,
                                                 DestIp>>
                          (N, 2, nodeId, featureMap, identifier);
  filter->registerConsumer(sumIncoming); 
  
  identifier = "serverSumOutgoingFlowSize";
  auto sumOutgoing = std::make_shared<ExponentialHistogramSum<size_t, Netflow,
                                                  DestTotalBytes,
                                                  DestIp>>
                          (N, 2, nodeId, featureMap, identifier);
  filter->registerConsumer(sumOutgoing);
     
  identifier = "serverVarianceIncomingFlowSize";
  auto varianceIncoming = std::make_shared<ExponentialHistogramVariance<
                               size_t, Netflow, SrcTotalBytes, DestIp>>
                          (N, 2, nodeId, featureMap, identifier);
  filter->registerConsumer(varianceIncoming); 
  
  identifier = "serverVarianceOutgoingFlowSize";
  auto varianceOutgoing = std::make_shared<ExponentialHistogramVariance<
                            size_t, Netflow, DestTotalBytes, DestIp>>
                              (N, 2, nodeId, featureMap, identifier);
  filter->registerConsumer(varianceOutgoing);
     
  if (!receiver.connect()) {
    std::cout << "Couldn't connected to " << ip << ":" << ncPort << std::endl;
    return -1;
  }

  ////////////////// Creating Time Lapse Series ///////////////////////
  //FeatureMap featureMap;
  
  #define DestIp_TimeLapseSeries    0
  #define SrcIp_TimeLapseSeries     1
  #define TimeDiff_TimeLapseSeries  2 
  typedef std::tuple<std::string, std::string, double> TimeLapseSeries;
 
  std::vector<Expression<Netflow>> expressions;
  std::vector<std::string> names;
  std::string name = "TimeLapseSeries_TimeDiff";
  names.push_back(name);
  
  // Expression TimeSeconds - Prev.TimeSeconds
  // 
  // TimeSeconds field token 
  std::shared_ptr<ExpressionToken<Netflow>> fieldToken = std::make_shared<
    FieldToken<TimeSeconds, Netflow>>(featureMap);
  // Sub operator token
  std::shared_ptr<ExpressionToken<Netflow>> subToken = std::make_shared<
    SubOperator<Netflow>>(featureMap);
  // Prev.TimeSeconds
  std::shared_ptr<ExpressionToken<Netflow>> prevToken = std::make_shared<
    PrevToken<TimeSeconds, Netflow>>(featureMap);
    
  std::list<std::shared_ptr<ExpressionToken<Netflow>>> infixList2;
  infixList2.push_back(fieldToken);
  infixList2.push_back(subToken);
  infixList2.push_back(prevToken);
    
  Expression<Netflow> expression(infixList2);
  expressions.push_back(expression); 
   
  TupleExpression<Netflow> tupleExpression(expressions, names);
  identifier = "destsrc_timelapseseries";

  auto timeLapseSeries = std::make_shared<TransformProducer<
                     Netflow, TimeLapseSeries, DestIp, SourceIp>>
                             (tupleExpression,
                              nodeId,
                              featureMap,
                              identifier,
                              queueLength);  

  filter->registerConsumer(timeLapseSeries);

  std::list<std::string> destSrcIdentifiers;

  identifier = "destSourceTimeDiffVariance";
  destSrcIdentifiers.push_back(identifier);
  auto destSourceTimeDiffVar = 
    std::make_shared<ExponentialHistogramVariance<size_t, TimeLapseSeries, 
                           TimeDiff_TimeLapseSeries, 
                           DestIp_TimeLapseSeries, SrcIp_TimeLapseSeries>>
                          (N, 2, nodeId, featureMap, identifier);
  timeLapseSeries->registerConsumer(destSourceTimeDiffVar); 

  identifier = "projectOutSource";
  auto projectToDest = 
    std::make_shared<Project<TimeLapseSeries, DestIp_TimeLapseSeries, 
                SrcIp_TimeLapseSeries,
                DestIp_TimeLapseSeries, SrcIp_TimeLapseSeries>>
                (destSrcIdentifiers, 
                nodeId,
                featureMap, 
                identifier);
               
  timeLapseSeries->registerConsumer(projectToDest);
 
  identifier = "serverAveClientsTimeDiffVar";
  auto aveFunction = [](std::list<std::shared_ptr<Feature>> myList)->double {
    double sum = 0;
    for (auto feature : myList) {
      sum = sum + feature->evaluate(valueFunc); 
    }
    return sum / myList.size();
  };

  auto destTimeDiffVar = 
    std::make_shared<CollapsedConsumer<Netflow, DestIp>>(
                                               aveFunction,
                                               "destSourceTimeDiffVariance",
                                               nodeId, 
                                               featureMap, 
                                               identifier); 
  
  filter->registerConsumer(destTimeDiffVar);
  
#ifdef DEBUG
  cout << "DEBUG: connected to receiver " << endl;
#endif

  milliseconds ms1 = duration_cast<milliseconds>(
    system_clock::now().time_since_epoch()
  );
  receiver.receive();
  milliseconds ms2 = duration_cast<milliseconds>(
    system_clock::now().time_since_epoch()
  );
  std::cout << "Seconds for Node" << nodeId << ": "  
    << static_cast<double>(ms2.count() - ms1.count()) / 1000 << std::endl;

}

