#define TIMING
#define DETAIL_TIMING
#define METRICS

#include <sam/GraphStore.hpp>
#include <sam/EdgeDescription.hpp>
#include <sam/SubgraphQuery.hpp>
#include <sam/ZeroMQPushPull.hpp>
#include <sam/NetflowGenerators.hpp>
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

typedef GraphStore<Netflow, NetflowTuplizer, SourceIp, DestIp,
                   TimeSeconds, DurationSeconds,
                   StringHashFunction, StringHashFunction,
                   StringEqualityFunction, StringEqualityFunction>
        GraphStoreType;

typedef GraphStoreType::QueryType SubgraphQueryType;

typedef GraphStoreType::EdgeDescriptionType EdgeDescriptionType;

typedef TupleStringHashFunction<Netflow, SourceIp> SourceHash;
typedef TupleStringHashFunction<Netflow, DestIp> TargetHash;
typedef ZeroMQPushPull<Netflow, NetflowTuplizer, SourceHash, TargetHash>
        PartitionType;

typedef GraphStoreType::ResultType ResultType;  

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
  size_t additionalNetflows; ///> Number of additional netflows
  std::string outputNetflowFile = ""; ///> Where netflows should be written
  size_t numPushSockets = 1;
  size_t numPullThreads = 1;
  size_t timeout = 1000;
  double dropTolerance;

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
    ("writeNetflows", po::value<std::string>(&outputNetflowFile),
      "If specified, will write out the generated netflows to a file")
    ("numPullThreads", po::value<size_t>(&numPullThreads)->default_value(1),
      "Number of pull threads (default 1)")
    ("numPushSockets", po::value<size_t>(&numPushSockets)->default_value(1),
      "Number of push sockets a node creates to talk to another node (default 1)")
    ("timeout", po::value<size_t>(&timeout)->default_value(1000),
      "How long (in ms) to wait before a send call fails")
    ("dropTolerance", po::value<double>(&dropTolerance)->default_value(1000),
      "How long (in seconds) this process can get behind before dropping.")
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

  vector<string> hostnames(numNodes); 
  vector<size_t> ports(numNodes); 

  if (numNodes == 1) { // Case when we are operating on one node
    hostnames[0] = "127.0.0.1";
    ports[0] = startingPort;
  } else {
    for (int i = 0; i < numNodes; i++) {
      hostnames[i] = prefix + boost::lexical_cast<string>(i);
      ports[i] = (startingPort + i);  
    }
  }

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
     timeWindow, featureMap);

  pushPull->registerConsumer(graphStore);

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
  query->finalize();

  graphStore->registerQuery(query);

  double time = 0.0;
  size_t triangleCounter = 0;

  double increment = 0.1;
  if (rate > 0) {
    increment = 1 / rate;
  }

  //Receiver
  //Receiver->receive()

  /*duration<double> time_space = duration_cast<duration<double>>(t2-t1);
  double totalTime = time_space.count(); 

  size_t numResults = (graphStore->getNumResults() < resultsCapacity) ?
    graphStore->getNumResults() : resultsCapacity;

  pushPull->terminate();
  */
  delete pushPull;

}
