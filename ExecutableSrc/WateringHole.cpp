/**
 * WateringHole.cpp
 * Query that looks for watering hole attackes.
 * 
 * Created: November 20, 2018
 * Author: elgood
 */

#include "GraphStore.hpp"
#include "EdgeDescription.hpp"
#include "SubgraphQuery.hpp"
#include "ZeroMQPushPull.hpp"
#include "ReadSocket.hpp"
#include "TopK.hpp"
#include <boost/program_options.hpp>
#include <string>
#include <vector>
#include <chrono>

using namespace sam;
namespace po = boost::program_options;
using namespace std::chrono;
using std::string;
using std::vector;

typedef GraphStore<Netflow, NetflowTuplizer, SourceIp, DestIp,
                   TimeSeconds, DurationSeconds,
                   StringHashFunction, StringHashFunction,
                   StringEqualityFunction, StringEqualityFunction>
        GraphStoreType;

typedef GraphStoreType::QueryType SubgraphQueryType;

typedef ZeroMQPushPull<Netflow, SourceIp, DestIp,
        NetflowTuplizer, StringHashFunction>
        PartitionType;

int main(int argc, char** argv)
{

  /// Parameters
  size_t numNodes; ///> The number of nodes in the cluster
  size_t nodeId; ///> The node id of this node
  string prefix; ///> The prefix to the nodes
  size_t startingPort; ///> The starting port number for push/pull sockets
  size_t hwm; ///> The high-water mark (Zeromq parameter)
  string ip; ///> The ip to read the nc data from.
  std::size_t ncPort; ///> The port to read the nc data from.
  size_t queueLength; ///> The length of the input queue
  size_t N; ///> The total number of elements in a sliding window
  size_t b; ///> The number of elements in a dormant or active window
  size_t k; ///> The number of elements to keep track of
  size_t capacity; ///> Capacity of FeatureMap and subscriber
  size_t graphCapacity; ///> For csc and csr
  size_t numPushSockets = 1;
  size_t numPullThreads = 1;
  size_t tableCapacity; ///> For SubgraphQueryResultMap intermediate results
  size_t resultsCapacity; ///> For final results
  double timeWindow; ///> For graphStore

  po::options_description desc("This looks for watering hole attacks"
    " in netflow data");
  desc.add_options()
    ("help", "help message")
    ("numNodes", po::value<size_t>(&numNodes)->default_value(1),
      "The number of nodes involved in the computation (default: 1).")
    ("nodeId", po::value<size_t>(&nodeId)->default_value(0),
      "The node id of this node (default: 0).")
    ("prefix", po::value<string>(&prefix)->default_value("node"),
      "The prefix common to all nodes (default is node, but localhost is"
      "used when there is only one node).")
    ("startingPort", po::value<size_t>(&startingPort)->default_value(
      10000), "The starting port for the zeromq communications")
    ("hwm", po::value<size_t>(&hwm)->default_value(10000),
      "The high water mark (how many items can queue up before we start "
      "dropping)")
    ("ip", po::value<string>(&ip)->default_value("localhost"),
      "The ip to receive the data from nc")
    ("ncPort", po::value<size_t>(&ncPort)->default_value(9999),
      "The port to receive the data from nc")
    ("queueLength", po::value<size_t>(&queueLength)->default_value(10000),
      "We fill a queue before sending things in parallel to all consumers."
      "  This controls the size of that queue.")
    ("N", po::value<size_t>(&N)->default_value(10000),
      "The total number of elements in a sliding window")
    ("b", po::value<size_t>(&b)->default_value(1000),
      "The number of elements per block (active or dynamic window)")
    ("k", po::value<size_t>(&k)->default_value(1000),
      "The number of top elements to keep track of per block.")
    ("capacity", po::value<std::size_t>(&capacity)->default_value(10000),
      "The capacity of the FeatureMap and FeatureSubcriber")
    ("graphCapacity",
      po::value<size_t>(&graphCapacity)->default_value(1000),
      "How many slots in the csr and csc (default: 1000).")
    ("numPullThreads", po::value<size_t>(&numPullThreads)->default_value(1),
      "Number of pull threads (default 1)")
    ("numPushSockets", po::value<size_t>(&numPushSockets)->default_value(1),
      "Number of push sockets a node creates to talk to another node "
      "(default 1)")
    ("tableCapacity",
      po::value<size_t>(&tableCapacity)->default_value(1000),
      "How many slots in SubgraphQueryResultMap and EdgeRequestMap "
      "(default 1000).")
    ("resultsCapacity",
      po::value<size_t>(&resultsCapacity)->default_value(1000),
      "How many results we keep around before tossing (default: 1000).")
    ("timeWindow",
      po::value<double>(&timeWindow)->default_value(100),
      "How big of a time window before intermediate results expire "
      "(default: 100).")
  ;

  double keepQueries =  1;

  // Parse the command line variables
  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 1;
  }

  // All the hosts in the cluster.  The names are created with a
  // concatenation of prefix with integer id of the node.
  vector<string> hostnames(numNodes);

  // The port numbers assigned to each node for zeromq push/pull
  // communication.  We start at startingPort and increase by one for each
  // node.
  vector<size_t> ports(numNodes);

  if (numNodes == 1) { // Case when we are operating on one node
    hostnames[0] = "127.0.0.1";
    ports[0] = startingPort;
  } else {
    for (int i = 0; i < numNodes; i++) {
      // Assumes all the host names can be composed by adding prefix with
      // [0,numNodes).
      hostnames[i] = prefix + boost::lexical_cast<string>(i);

      // Assigns ports starting at startingPort and increments.  These ports
      // are used for zeromq push/pull sockets.
      ports[i] = (startingPort + i);
    }
  }

  auto featureMap = std::make_shared<FeatureMap>(capacity);

  auto receiver = std::make_shared<ReadSocket>(ip, ncPort);
  size_t timeout = 1000;

  // Setting up the ZeroMQPushPull object
  auto pushPull = std::make_shared<PartitionType>(queueLength,
                                    numNodes, nodeId,
                                    hostnames,
                                    startingPort, timeout, false,
                                    hwm);

  receiver->registerConsumer(pushPull);

  std::string topkId = "topk";
  auto topk = std::make_shared<
    TopK<Netflow, DestIp>>(N, b, k, nodeId, featureMap, topkId);
    
  pushPull->registerConsumer(topk);

  auto graphStore = std::make_shared<GraphStoreType>(
     numNodes, nodeId,
     hostnames, startingPort + numNodes,
     hwm, graphCapacity,
     tableCapacity, resultsCapacity,
     numPushSockets, numPullThreads, timeout,
     timeWindow, keepQueries, featureMap);

  // Set up GraphStore object to get input from ZeroMQPushPull objects
  pushPull->registerConsumer(graphStore);

  std::string e0 = "e0";
  std::string e1 = "e1";
  std::string bait = "bait";
  std::string target = "target";
  std::string controller = "controller";

  // Set up the query
  EdgeFunction starttimeFunction = EdgeFunction::StartTime;
  EdgeFunction endtimeFunction = EdgeFunction::EndTime;
  EdgeOperator greaterEdgeOperator = EdgeOperator::GreaterThan;
  EdgeOperator lessEdgeOperator = EdgeOperator::LessThan;
  EdgeOperator equalEdgeOperator = EdgeOperator::Assignment;

  EdgeExpression target2Bait(target, e0, bait);
  EdgeExpression target2Controller(target, e1, controller);
  TimeEdgeExpression endE0Second(endtimeFunction, e0,
                                 equalEdgeOperator, 0);
  TimeEdgeExpression startE1First(starttimeFunction, e1,
                                  greaterEdgeOperator, 0);
  TimeEdgeExpression startE1Second(starttimeFunction, e1,
                                  lessEdgeOperator, 10);

  //bait in Top1000
  VertexConstraintExpression baitTopK(bait, VertexOperator::In, topkId);
  
  //controller not in Top1000
  VertexConstraintExpression controllerNotTopK(controller, 
                                               VertexOperator::NotIn, topkId);

  SubgraphQueryType query(featureMap);
  query.addExpression(target2Bait);
  query.addExpression(target2Controller);
  query.addExpression(endE0Second);
  query.addExpression(startE1First);
  query.addExpression(startE1Second);
  query.addExpression(baitTopK);
  query.addExpression(controllerNotTopK);

  milliseconds ms1 = duration_cast<milliseconds>(
    system_clock::now().time_since_epoch()
  );
  receiver->receive();
  milliseconds ms2 = duration_cast<milliseconds>(
    system_clock::now().time_since_epoch()
  ); 

  size_t numResults = graphStore->getNumResults();
  std::cout << "Number of results " << numResults << std::endl;


}

