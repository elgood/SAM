/*
 * RunTrianglesComplex.cpp
 * This creates many more triangles than the simple test.  It has a pool of
 * n vertices and creates edges within that pool.  
 *
 *  Created on: April 16, 2018
 *      Author: elgood
 */

//#define DEBUG
#define TIMING
#define DETAIL_TIMING
#define METRICS
#define DROP_QUERIES
//#define DETAIL_METRICS
//#define NOBLOCK
//#define NOBLOCK_WHILE
//#define DETAIL_METRICS2

#include <sam/GraphStore.hpp>
#include <sam/EdgeDescription.hpp>
#include <sam/SubgraphQuery.hpp>
#include <sam/ZeroMQPushPull.hpp>
#include <sam/VastNetflowGenerators.hpp>
#include <boost/program_options.hpp>
#include <string>
#include <vector>
#include <chrono>
#include <fstream>

using namespace sam;
namespace po = boost::program_options;
using std::string;
using std::vector;
using namespace std::chrono;

typedef GraphStore<VastNetflow, VastNetflowTuplizer, SourceIp, DestIp,
                   TimeSeconds, DurationSeconds,
                   StringHashFunction, StringHashFunction,
                   StringEqualityFunction, StringEqualityFunction>
        GraphStoreType;

typedef GraphStoreType::QueryType SubgraphQueryType;

typedef GraphStoreType::EdgeDescriptionType EdgeDescriptionType;

typedef TupleStringHashFunction<VastNetflow, SourceIp> SourceHash;
typedef TupleStringHashFunction<VastNetflow, DestIp> TargetHash;
typedef ZeroMQPushPull<VastNetflow, VastNetflowTuplizer, SourceHash, TargetHash>
        PartitionType;

typedef GraphStoreType::ResultType ResultType;  

void printStuff(std::shared_ptr<GraphStoreType> graphStore, size_t nodeId) {
  #ifdef TIMING
  printf("Node %lu Timing total consume time: %f\n", nodeId, 
    graphStore->getTotalTimeConsume());
  #endif

  #ifdef DETAIL_TIMING
  printf("Node %lu Detail Timing ConsumeDoesTheWork::addEdge: %f\n", nodeId,
    graphStore->getTotalTimeConsumeAddEdge());
  printf("Node %lu Detail Timing ConsumeDoesTheWork::resultMap->process: %f\n", 
    nodeId, graphStore->getTotalTimeConsumeResultMapProcess());
  printf("Node %lu Detail Timing ConsumeDoesTheWork::edgeRequestMap->process:"
    " %f\n", nodeId, graphStore->getTotalTimeConsumeEdgeRequestMapProcess());
  printf("Node %lu Detail Timing ConsumeDoesTheWork::checkSubgraphQueries: %f"
    "\n", nodeId, graphStore->getTotalTimeConsumeCheckSubgraphQueries());
  printf("Node %lu Detail Timing ConsumeDoesTheWork::processEdgeRequests: %f\n",
    nodeId, graphStore->getTotalTimeConsumeProcessEdgeRequests());
  printf("Node %lu Detail Timing edgeCallback::totalTimeEdgeCallbackResultMap"
    "Process: %f\n", nodeId, 
    graphStore->getTotalTimeEdgeCallbackResultMapProcess());
  printf("Node %lu Detail Timing edgeCallback::totalTimeEdgeCallbackProcessEdge"
    "Requests: %f\n", nodeId, 
    graphStore->getTotalTimeEdgeCallbackProcessEdgeRequests());
  printf("Node %lu Detail Timing requestCallback::totalTimeRequestCallback"
    "ResultMapProcess: %f\n", nodeId, 
    graphStore->getTotalTimeRequestCallbackAddRequest());
  printf("Node %lu Detail Timing reqeustCallback::totalTimeRequestCallback"
    "ProcessAgainstGraph: %f\n", nodeId, 
    graphStore->getTotalTimeRequestCallbackProcessAgainstGraph());
  
  ///// EdgeRequestMap timing details //////////////////////////////
  printf("Node %lu Detail Timing EdgeRequestMap::totalTimeLock %f\n",
    nodeId, graphStore->getTotalTimeEdgeRequestMapLock());
  printf("Node %lu Detail Timing EdgeRequestMap::totalTimePush %f\n",
    nodeId, graphStore->getTotalTimeEdgeRequestMapPush());
  ///// End EdgeRequestMap timing details ////////////////////////

  printf("Node %lu Detail Timing total processAgainstGraph time: %f\n",
    nodeId, graphStore->getTotalTimeProcessAgainstGraph());
  printf("Node %lu Detail Timing total processSource time: %f\n",
    nodeId, graphStore->getTotalTimeProcessSource());
  printf("Node %lu Detail Timing total processTarget time: %f\n",
    nodeId, graphStore->getTotalTimeProcessTarget());
  printf("Node %lu Detail Timing total processSourceTarget time: %f\n",
    nodeId, graphStore->getTotalTimeProcessSourceTarget());
  printf("Node %lu Detail Timing total processProcessAgainstGraph time: "
    "%f\n", nodeId, graphStore->getTotalTimeProcessProcessAgainstGraph());
  printf("Node %lu Detail Timing total processLoop1 time: %f\n",
    nodeId, graphStore->getTotalTimeProcessLoop1());
  printf("Node %lu Detail Timing total processLoop2 time: %f\n",
    nodeId, graphStore->getTotalTimeProcessLoop2());
  #endif

  #ifdef METRICS
  /////// EdgeRequestMap metrics /////////////////////////////////////
  printf("Node %lu Metrics total EdgeRequestMap edge push attempts: %lu\n", nodeId,
    graphStore->getTotalEdgeRequestMapPushes());
  printf("Node %lu Metrics total EdgeRequestMap edge push fails: %lu\n", nodeId,
    graphStore->getTotalEdgeRequestMapPushFails());
  printf("Node %lu Metrics total EdgeRequestMap edge requests viewed: %lu\n", nodeId,
    graphStore->getTotalEdgeRequestMapRequestsViewed());
  /////// End EdgeRequestMap metrics /////////////////////////////////


  printf("Node %lu ResultMap results added: %lu\n", nodeId,
    graphStore->getTotalResultsCreatedInResultMap());
  printf("Node %lu ResultMap results deleted: %lu\n", nodeId,
    graphStore->getTotalResultsDeletedInResultMap());
  printf("Node %lu Csr edges added: %lu\n", nodeId,
    graphStore->getTotalEdgesAddedInCsr());
  printf("Node %lu Csr edges deleted: %lu\n", nodeId,
    graphStore->getTotalEdgesDeletedInCsr());
  printf("Node %lu Csc edges added: %lu\n", nodeId,
    graphStore->getTotalEdgesAddedInCsc());
  printf("Node %lu Csc edges deleted: %lu\n", nodeId,
    graphStore->getTotalEdgesDeletedInCsc());
  printf("Node %lu total GraphStore edge push attempts: %lu\n", nodeId,
    graphStore->getTotalEdgePushes());
  printf("Node %lu total GraphStore edge push fails: %lu\n", nodeId,
    graphStore->getTotalEdgePushFails());
  printf("Node %lu total GraphStore request push attempts: %lu\n", nodeId,
    graphStore->getTotalRequestPushes());
  printf("Node %lu total GraphStore request fails: %lu\n", nodeId,
    graphStore->getTotalRequestPushFails());

  #endif

  #ifdef DETAIL_TIMING
  /*std::list<double> const& consumeTimes = graphStore->getConsumeTimes();
  double average = std::accumulate(consumeTimes.begin(),
                               consumeTimes.end(), 0.0) / consumeTimes.size();

  std::list<double> diff;
  std::transform(consumeTimes.begin(), consumeTimes.end(), diff.begin(),
                 std::bind2nd(std::minus<double>(), average));
  double sq_sum = std::inner_product(diff.begin(), diff.end(), diff.begin(), 0);
  double stdev = std::sqrt(sq_sum / consumeTimes.size());
  size_t i = 0;
  printf("Node %lu expected iteration time %f\n", nodeId, 1.0/rate);
  for (double t : consumeTimes) {
    if (t > average + 2 * stdev) {
      printf("Node %lu Consume time outside 2 stddev (i, time): %lu %f\n", 
        nodeId, i, t);
    }
    i++;
  }*/
  #endif
 
}

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
  size_t numVertices; ///> How many vertices in the graph
  size_t numNetflows; ///> How netflows to generate
  double queryTimeWindow; ///> Amount of time within a triangle can occur.
  size_t numThreads; ///> Number of threads for for loops
  double rate; ///> Netflows per second
  bool check;
  size_t additionalNetflows; ///> Number of additional netflows
  std::string outputNetflowFile = ""; ///> Where netflows should be written
  size_t numPushSockets = 1;
  size_t numPullThreads = 1;
  size_t timeout = 1000;
  double dropTolerance;
  double keepQueries;

  po::options_description desc("This code creates a set of vertices "
    " and generates edges amongst that set.  It finds triangles among the"
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
    ("numVertices",
      po::value<size_t>(&numVertices)->default_value(1000),
      "How many vertices are in the graph.")
    ("numNetflows",
      po::value<size_t>(&numNetflows)->default_value(10000),
      "How many netflows to generate (default: 10000")
    ("additionalNetflows",
      po::value<size_t>(&additionalNetflows)->default_value(1000),
      "Number of additional netflows to add at the end to flush out results.")
    ("rate",
      po::value<double>(&rate)->default_value(100),
      "Rate at which netflows are provided.")
    ("check", po::bool_switch(&check)->default_value(false),
      "Performs check of results")
    ("writeNetflows", po::value<std::string>(&outputNetflowFile),
      "If specified, will write out the generated netflows to a file")
    ("numPullThreads", po::value<size_t>(&numPullThreads)->default_value(1),
      "Number of pull threads (default 1)")
    ("numPushSockets", po::value<size_t>(&numPushSockets)->default_value(1),
      "Number of push sockets a node creates to talk to another node "
      "(default 1)")
    ("timeout", po::value<size_t>(&timeout)->default_value(1000),
      "How long (in ms) to wait before a send call fails")
    ("dropTolerance", po::value<double>(&dropTolerance)->default_value(1000),
      "How long (in seconds) this process can get behind before dropping.")
    ("keepQueries", po::value<double>(&keepQueries)->default_value(1.0),
      "Percentage of checks aginst queries to keep") 
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

  std::ofstream ofile;
  if (outputNetflowFile != "") {
    ofile.open(outputNetflowFile);
  }

  srand(nodeId);

  // Setting up the random pool generator
  AbstractVastNetflowGenerator* generator = new RandomPoolGenerator(numVertices);
  
  // Used at the end to clear things out
  AbstractVastNetflowGenerator *otherGenerator = new RandomGenerator(); 

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
                                    hostnames,
                                    startingPort, timeout, false, 
                                    hwm);

  auto featureMap = std::make_shared<FeatureMap>(1000);

  auto graphStore = std::make_shared<GraphStoreType>(
     numNodes, nodeId,
     hostnames, startingPort + numNodes, 
     hwm, graphCapacity,
     tableCapacity, resultsCapacity, 
     numPushSockets, numPullThreads, timeout,
     timeWindow, keepQueries, featureMap);

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
  TimeEdgeExpression startE0First(starttimeFunction, e0, 
                                  equal_edge_operator, 0);
  TimeEdgeExpression startE1First(starttimeFunction,e1,
                                  greater_edge_operator, 0);
  TimeEdgeExpression startE2First(starttimeFunction,e2,
                                  greater_edge_operator, 0);
  TimeEdgeExpression startE0Second(starttimeFunction, e0, 
                                   less_edge_operator,queryTimeWindow);
  TimeEdgeExpression startE1Second(starttimeFunction,e1,
                                   less_edge_operator,queryTimeWindow);
  TimeEdgeExpression startE2Second(starttimeFunction,e2,
                                   less_edge_operator, queryTimeWindow);
  //TimeEdgeExpression endE0Second(endtimeFunction, e0, 
  //                                 less_edge_operator,queryTimeWindow);
  //TimeEdgeExpression endE1Second(endtimeFunction,e1,
  //                                 less_edge_operator,queryTimeWindow);
  //TimeEdgeExpression endE2Second(endtimeFunction,e2,
  //                                 less_edge_operator, queryTimeWindow);
  /*TimeEdgeExpression endE0First(endtimeFunction, e0, greater_edge_operator, 0);
  TimeEdgeExpression endE0Second(endtimeFunction, e0, less_edge_operator, 
    queryTimeWindow);
  TimeEdgeExpression endE1First(endtimeFunction, e1, greater_edge_operator, 0);
  TimeEdgeExpression endE1Second(endtimeFunction, e1, less_edge_operator, 
    queryTimeWindow);
  TimeEdgeExpression endE2First(endtimeFunction, e2, greater_edge_operator, 0);
  TimeEdgeExpression endE2Second(endtimeFunction, e2, less_edge_operator,
    queryTimeWindow);*/

  auto query = std::make_shared<SubgraphQueryType>(featureMap);
  query->addExpression(x2y);
  query->addExpression(y2z);
  query->addExpression(z2x);
  query->addExpression(startE0First);
  query->addExpression(startE1First);
  query->addExpression(startE2First);
  query->addExpression(startE0Second);
  query->addExpression(startE1Second);
  query->addExpression(startE2Second);
  //query.addExpression(endE0Second);
  //query.addExpression(endE1Second);
  //query.addExpression(endE2Second);
  query->finalize();

  graphStore->registerQuery(query);

  //pushPull->acceptData();

  double time = 0.0;
  size_t triangleCounter = 0;

  double increment = 0.1;
  if (rate > 0) {
    increment = 1 / rate;
  }

  auto t1 = std::chrono::high_resolution_clock::now();

  size_t numDropped = 0;

  for(size_t i = 0; i < numNetflows; i++)
  {
    bool drop = false;
    DEBUG_PRINT("NodeId %lu generating tuple i %lu\n", nodeId, i);
    if (i % 10000 == 0) {
      auto currenttime = std::chrono::high_resolution_clock::now();
      double expectedTime = i * increment;
      double actualTime = 
        duration_cast<duration<double>>(currenttime - t1).count();
      printf("Node %lu RunTriangle iteration %lu.  Expected time: %f"
        " Actual time: %f\n", nodeId, i, expectedTime, actualTime);
    }
    if (rate > 0) {
      auto currenttime = std::chrono::high_resolution_clock::now();
      double diff = duration_cast<duration<double>>(currenttime - t1).count();
      if (diff < i * increment) {
        size_t numMilliseconds = (i * increment - diff) * 1000;
        std::this_thread::sleep_for(
          std::chrono::milliseconds(numMilliseconds));
      } else {
        if (diff - i * increment > 0.1) {
          DEBUG_PRINT("Node %lu Regular tuple behind by %f\n", nodeId,
            diff - i * increment);
        }
        if (diff - i * increment > 1) {
           DEBUG_PRINT("Node %lu way behind (%f) \n", 
             nodeId, diff - i * increment);
           //printStuff(graphStore, nodeId);
           //exit(1);
        }
        if (diff - i * increment > dropTolerance) {
          drop = true;
        }
      }
    }

    if (!drop) {
      std::string str = generator->generate(time);
      if (ofile.is_open()) {
        DEBUG_PRINT("Node %lu writing tuple %s\n", nodeId, str.c_str());
        ofile << str << std::endl;
      }
      time += increment;

      try {
        auto consumeTime1 = std::chrono::high_resolution_clock::now();
        pushPull->consume(str);
        auto consumeTime2 = std::chrono::high_resolution_clock::now();
        double consumeTime = duration_cast<duration<double>>(
          consumeTime2 - consumeTime1).count();
        if (consumeTime > 0.1) {
          printf("Node %lu warning consume took %f\n", nodeId, consumeTime);
        }
      } catch (std::bad_alloc e) {
        printf("Node %lu caught a bad_alloc exception after calling "
          "pushPull->consume(str) where str = %s\n", nodeId, str.c_str());
      }
    } else {
      numDropped++;
    }
  }
  auto t2 = std::chrono::high_resolution_clock::now();

  for(size_t i = 0; i < additionalNetflows; i++) {
    DEBUG_PRINT("NodeId %lu generating additional tuple i %lu\n", nodeId, i);
    std::string str = otherGenerator->generate(time);
    ofile << str << std::endl;
    time += increment;
    pushPull->consume(str);
  }
  ofile.close();

  duration<double> time_space = duration_cast<duration<double>>(t2-t1);
  double totalTime = time_space.count(); 
  printf("Node %lu Time: %f seconds\n", nodeId, totalTime);
  printf("Node %lu Experimental rate: %f\n", nodeId, 
    static_cast<double>(numNetflows) / totalTime);
  printf("Node %lu Specified rate: %f\n", nodeId, rate);  

  printf("Node %lu found %lu triangles\n",
    nodeId, graphStore->getNumResults());
  printf("Node %lu num dropped netflows %lu\n", nodeId, numDropped);

  size_t numResults = (graphStore->getNumResults() < resultsCapacity) ?
    graphStore->getNumResults() : resultsCapacity;

  printf("Node %lu total GraphStore edge push attempts: %lu\n", nodeId,
    graphStore->getTotalEdgePushes());
  printf("Node %lu total GraphStore edge push fails: %lu\n", nodeId,
    graphStore->getTotalEdgePushFails());

  pushPull->terminate();
  
  printStuff(graphStore, nodeId);
 
  if (check) {
    for(size_t i = 0; i < numResults; i++)
    {
      ResultType result = graphStore->getResult(i);
      //printf("%s\n", result.toString().c_str());
      VastNetflow n0 = result.getResultTuple(0);
      VastNetflow n1 = result.getResultTuple(1);
      VastNetflow n2 = result.getResultTuple(2);
      double starttime0 = std::get<TimeSeconds>(n0);
      double starttime1 = std::get<TimeSeconds>(n1);
      double starttime2 = std::get<TimeSeconds>(n2);
      if (starttime0 > starttime1) {
        printf("problem startime0 > starttime1 %s\n", 
          result.toString().c_str());
      }
      if (starttime1 > starttime2) {
        printf("problem startime1 > starttime2 %s\n", 
          result.toString().c_str());
      }
      if (starttime2 - starttime0 >= queryTimeWindow ) {
        printf("problem startime2 - starttime0 > %f %s\n", queryTimeWindow,
          result.toString().c_str());
      }
    }
  }

  delete pushPull;
  delete generator;
  delete otherGenerator;

}
