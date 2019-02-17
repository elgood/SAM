/*
 * RunTrianglesSimple.cpp
 * Creates a number of triangles to test out scalability of subgraph queries.
 * This tests creates a bunch of random edges with random ips.  However,
 * at a specified interval it will create a triangle.  All the edges for
 * a triangle are only used once (i.e. if an edge e is in triangle T, then
 * e is not an edge in any other triangle).
 *
 *  Created on: April 14, 2018
 *      Author: elgood
 */

//#define DEBUG

#include <sam/GraphStore.hpp>
#include <sam/EdgeDescription.hpp>
#include <sam/SubgraphQuery.hpp>
#include <sam/ZeroMQPushPull.hpp>
#include <sam/NetflowGenerators.hpp>
#include <boost/program_options.hpp>
#include <string>
#include <vector>
#include <chrono>

using namespace sam;
namespace po = boost::program_options;
using std::string;
using std::vector;
using namespace std::chrono;

typedef GraphStore<Netflow, NetflowTuplizer, SourceIp, DestIp,
                   TimeSeconds, DurationSeconds,
                   StringHashFunction, StringHashFunction,
                   StringEqualityFunction, StringEqualityFunction>
        GraphStoreType;

typedef GraphStoreType::QueryType SubgraphQueryType;

typedef GraphStoreType::EdgeDescriptionType EdgeDescriptionType;

typedef ZeroMQPushPull<Netflow, SourceIp, DestIp, 
        NetflowTuplizer, StringHashFunction>
        PartitionType;



int main(int argc, char** argv) {

  /// Parameters
  size_t numNodes; ///> The number of nodes in the cluster
  size_t nodeId; ///> The node id of this node
  string prefix; ///> The prefix to the nodes
  size_t startingPort; ///> The starting port number for push/pull sockets
  size_t hwm; ///> The high-water mark (Zeromq parameter)
  size_t queueLength; ///> The length of the input queue
  size_t graphCapacity; ///> For csc and csr
  size_t tableCapacity; ///> For SubgraphQueryResultMap intermediate results
  size_t resultsCapacity; ///> For final results
  double timeWindow; ///> For graphStore
  size_t numTriangles; ///> How many triangles to generate
  size_t numNetflows; ///> How netflows to generate

  po::options_description desc("This code creates a specified number of"
    " number of triangles along with some random background traffic.");
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
    ("queueLength", po::value<size_t>(&queueLength)->default_value(1000),
      "We fill a queue before sending things in parallel to all consumers."
      "  This controls the size of that queue. (default: 1000)")
    ("graphCapacity", 
      po::value<size_t>(&graphCapacity)->default_value(1000),
      "How many slots in the csr and csc (default: 1000).")
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
    ("numTriangles",
      po::value<size_t>(&numTriangles)->default_value(1000),
      "How many triangles to create (default: 1000).  To make things "
      "simple, make sure numNetflows is divisible by numTriangles")
    ("numNetflows",
      po::value<size_t>(&numNetflows)->default_value(10000),
      "How many netflows to generate (default: 10000")
  ;

  // Parse the command line variables
  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  // Print out the help and exit if --help was specified.
  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 1;
  }

  // To make things simpler, make sure numTriangles evenly divices numTuples.
  size_t modValue = numNetflows / numTriangles;

  // Setting up the random netflow generator
  AbstractNetflowGenerator* generator = new RandomGenerator();

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

  //TODO Make a commandline parameter
  size_t timeout = 1000;

  // Setting up the ZeroMQPushPull object
  PartitionType* pushPull = new PartitionType(queueLength,
                                    numNodes, nodeId,
                                    hostnames, 
                                    startingPort, timeout, false,
                                    hwm);

  size_t numPushSockets = 1;
  size_t numPullThreads = 1;

  auto featureMap = std::make_shared<FeatureMap>(1000);

  auto graphStore = std::make_shared<GraphStoreType>(
     numNodes, nodeId,
     hostnames, startingPort + numNodes,
     hwm, graphCapacity,
     tableCapacity, resultsCapacity, 
     numPushSockets, numPullThreads, timeout,
     timeWindow, featureMap);

  // Set up GraphStore object to get input from ZeroMQPushPull objects
  pushPull->registerConsumer(graphStore);

  // Set up the triangle query
  EdgeFunction starttimeFunction = EdgeFunction::StartTime;
  EdgeFunction endtimeFunction = EdgeFunction::EndTime;
  EdgeOperator equal_edge_operator = EdgeOperator::Assignment;
  EdgeOperator greater_edge_operator = EdgeOperator::GreaterThan;

  std::string e0 = "e0";
  std::string e1 = "e1";
  std::string e2 = "e2";
  std::string nodex = "nodex";
  std::string nodey = "nodey";
  std::string nodez = "nodez";

  EdgeExpression x2y(nodex, e0, nodey);
  EdgeExpression y2z(nodey, e1, nodez);
  EdgeExpression z2x(nodez, e2, nodex);
  TimeEdgeExpression startE0Both(starttimeFunction, e0, equal_edge_operator, 0);
  TimeEdgeExpression startE1Both(starttimeFunction,e1,greater_edge_operator, 0);
  TimeEdgeExpression startE2Both(starttimeFunction,e2,greater_edge_operator, 0);

  auto query = std::make_shared<SubgraphQueryType>(featureMap);
  query->addExpression(x2y);
  query->addExpression(y2z);
  query->addExpression(z2x);
  query->addExpression(startE0Both);
  query->addExpression(startE1Both);
  query->addExpression(startE2Both);
  query->finalize();

  graphStore->registerQuery(query);

  double time = 0.0;
  double increment = 0.1;
  size_t triangleCounter = 0;

  auto t1 = std::chrono::high_resolution_clock::now();

  for(size_t i = 0; i < numNetflows; i++)
  {
    if (i % 1000 == 0) {
      printf("RunTriangle iteration %lu\n", i);
    }
    std::string str = generator->generate(time);
    time += increment;
  
    if (i % modValue == 0) {
      std::string nodex = "nodex_" +
        boost::lexical_cast<std::string>(triangleCounter) +
        "_" + boost::lexical_cast<std::string>(nodeId);
      std::string nodey = "nodey_" +
        boost::lexical_cast<std::string>(triangleCounter) +
        "_" + boost::lexical_cast<std::string>(nodeId);
      std::string nodez = "nodez_" +
        boost::lexical_cast<std::string>(triangleCounter) +
        "_" + boost::lexical_cast<std::string>(nodeId);

      Netflow netflow0 = makeNetflow(0, str);
      std::get<SourceIp>(netflow0) = nodex;
      std::get<DestIp>(netflow0) = nodey;

      // We create a triangle by adding two more edges.
      std::string str1 = generator->generate(time);
      time += increment;
      std::string str2 = generator->generate(time);
      time += increment;
      Netflow netflow1 = makeNetflow(0, str1);
      Netflow netflow2 = makeNetflow(0, str2);
      std::get<SourceIp>(netflow1) = nodey;
      std::get<DestIp>(netflow1) = nodez;
      std::get<SourceIp>(netflow2) = nodez;
      std::get<DestIp>(netflow2) = nodex;

      std::string str0 = sam::toString(netflow0);
      str1 = sam::toString(netflow1);
      str2 = sam::toString(netflow2);

      // Remove the id at the begining
      str0 = str0.substr(2);
      str1 = str1.substr(2);
      str2 = str2.substr(2);

      pushPull->consume(str0);
      pushPull->consume(str1);
      pushPull->consume(str2);

      triangleCounter++;

    } else {
      pushPull->consume(str);
    }
  }
  auto t2 = std::chrono::high_resolution_clock::now();
  duration<double> time_space = duration_cast<duration<double>>(t2-t1);
  std::cout << "Time: " << time_space.count() << " seconds" << std::endl;

  printf("Node %lu generated %lu triangles and found %lu triangles\n",
    nodeId, numTriangles, graphStore->getNumResults());

  delete pushPull;
  delete generator;
}
