/*
 * RunTrianglesComplex.cpp
 * This creates many more triangles than the simple test.  It has a pool of
 * n vertices and creates edges within that pool.  
 *
 *  Created on: April 16, 2018
 *      Author: elgood
 */

//#define DEBUG

#include "GraphStore.hpp"
#include "EdgeDescription.hpp"
#include "SubgraphQuery.hpp"
#include "ZeroMQPushPull.hpp"
#include "NetflowGenerators.hpp"
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

typedef GraphStoreType::ResultType ResultType;  

int main(int argc, char** argv) {

  srand (time(NULL));

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
  size_t numVertices; ///> How many vertices in the graph
  size_t numNetflows; ///> How netflows to generate
  double queryTimeWindow; ///> Amount of time within a triangle can occur.
  double timeIncrement; ///> Time between edge creation

  po::options_description desc("This code creates a set of vertices "
    " and generates edges amongst that set.  It finds triangels among the"
    " edges");
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
    ("queryTimeWindow",
      po::value<double>(&queryTimeWindow)->default_value(10),
      "Time window for the query to be satisfied (default: 10).")
    ("timeIncrement",
      po::value<double>(&timeIncrement)->default_value(0.1),
      "Time in seconds between edge creations (default 0.1).")
    ("numVertices",
      po::value<size_t>(&numVertices)->default_value(1000),
      "How many vertices are in the graph.")
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

  // Setting up the random pool generator
  AbstractNetflowGenerator* generator = new RandomPoolGenerator(numVertices);
  
  // Used at the end to clear things out
  AbstractNetflowGenerator *otherGenerator = new RandomGenerator(); 

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

  // Setting up the ZeroMQPushPull object
  PartitionType* pushPull = new PartitionType(queueLength,
                                    numNodes, nodeId,
                                    hostnames, ports,
                                    hwm);

  std::vector<size_t> requestPorts(numNodes);
  std::vector<size_t> edgePorts(numNodes);
  
  for(size_t i = 0; i < numNodes; i++) {
    requestPorts[i] = startingPort + numNodes   + i;
    edgePorts[i]    = startingPort + 2*numNodes + i; 
  }

  auto graphStore = std::make_shared<GraphStoreType>(
     numNodes, nodeId,
     hostnames, requestPorts,
     hostnames, edgePorts,
     hwm, graphCapacity,
     tableCapacity, resultsCapacity, timeWindow);

  // Set up GraphStore object to get input from ZeroMQPushPull objects
  pushPull->registerConsumer(graphStore);

  // Set up the triangle query
  EdgeFunction starttimeFunction = EdgeFunction::StartTime;
  EdgeFunction endtimeFunction = EdgeFunction::EndTime;
  EdgeOperator equal_edge_operator = EdgeOperator::Assignment;
  EdgeOperator greater_edge_operator = EdgeOperator::GreaterThan;
  EdgeOperator less_edge_operator = EdgeOperator::LessThan;

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
  TimeEdgeExpression endE0First(endtimeFunction, e0, greater_edge_operator, 0);
  TimeEdgeExpression endE0Second(endtimeFunction, e0, less_edge_operator, 
    queryTimeWindow);
  TimeEdgeExpression endE1First(endtimeFunction, e1, greater_edge_operator, 0);
  TimeEdgeExpression endE1Second(endtimeFunction, e1, less_edge_operator, 
    queryTimeWindow);
  TimeEdgeExpression endE2First(endtimeFunction, e2, greater_edge_operator, 0);
  TimeEdgeExpression endE2Second(endtimeFunction, e2, less_edge_operator,
    queryTimeWindow);

  SubgraphQueryType query;
  query.addExpression(x2y);
  query.addExpression(y2z);
  query.addExpression(z2x);
  query.addExpression(startE0Both);
  query.addExpression(startE1Both);
  query.addExpression(startE2Both);
  query.addExpression(endE0First);
  query.addExpression(endE0Second);
  query.addExpression(endE1First);
  query.addExpression(endE1Second);
  query.addExpression(endE2First);
  query.addExpression(endE2Second);
  query.finalize();

  graphStore->registerQuery(query);

  double time = 0.0;
  double increment = timeIncrement;
  size_t triangleCounter = 0;

  auto t1 = std::chrono::high_resolution_clock::now();

  for(size_t i = 0; i < numNetflows; i++)
  {
    if (i % 1000 == 0) {
      printf("RunTriangle iteration %lu\n", i);
    }
    std::string str = generator->generate(time);
    time += increment;
    pushPull->consume(str);
  }

  for(size_t i = 0; i < 100; i++) {
    std::string str = otherGenerator->generate(time);
    time += increment;
    pushPull->consume(str);
  }
  
  auto t2 = std::chrono::high_resolution_clock::now();
  duration<double> time_space = duration_cast<duration<double>>(t2-t1);
  std::cout << "Time: " << time_space.count() << " seconds" << std::endl;

  printf("Node %lu found %lu triangles\n",
    nodeId, graphStore->getNumResults());

  size_t numResults = (graphStore->getNumResults() < resultsCapacity) ?
    graphStore->getNumResults() : resultsCapacity;

  for(size_t i = 0; i < numResults; i++)
  {
    ResultType result = graphStore->getResult(i);
    //printf("%s\n", result.toString().c_str());
    Netflow n0 = result.getResultTuple(0);
    Netflow n1 = result.getResultTuple(1);
    Netflow n2 = result.getResultTuple(2);
    double starttime0 = std::get<TimeSeconds>(n0);
    double starttime1 = std::get<TimeSeconds>(n1);
    double starttime2 = std::get<TimeSeconds>(n2);
    if (starttime0 > starttime1) {
      printf("problem startime0 > starttime1 %s\n", result.toString().c_str());
    }
    if (starttime1 > starttime2) {
      printf("problem startime1 > starttime2 %s\n", result.toString().c_str());
    }
    if (starttime2 - starttime0 >= queryTimeWindow ) {
      printf("problem startime2 - starttime0 > %f %s\n", queryTimeWindow,
        result.toString().c_str());
    }
  }

  delete pushPull;
  delete generator;
  delete otherGenerator;
}
