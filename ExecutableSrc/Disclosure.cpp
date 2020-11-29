/*
 * Disclosure.cpp
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

#include <sam/sam.hpp>
#include <sam/tuples/VastNetflow.hpp>

//#define DEBUG 1

using std::string;
using std::vector;
using std::cout;
using std::endl;

namespace po = boost::program_options;

using namespace sam;
using namespace sam::vast_netflow;
using namespace std::chrono;


template<typename EdgeType, 
         typename Tuplizer, typename PartitionType, typename ProducerType>
void createPipeline(std::shared_ptr<ProducerType> producer,
            std::shared_ptr<FeatureMap> featureMap,
            std::shared_ptr<FeatureSubscriber> subscriber,
            size_t numNodes,
            size_t nodeId,
            std::vector<std::string> hostnames,
            size_t startingPort,
            size_t hwm,
            size_t graphCapacity,
            size_t tableCapacity,
            size_t resultsCapacity,
            size_t numSockets,
            size_t numPullThreads,
            size_t timeout,
            double timeWindow,
            size_t queueLength,
            std::string printerLocation)
{

  // TODO: Label
  // An operator to get the label from each netflow and add it to the
  // subscriber.
  string identifier = "label";

  // Doesn't really need a key, but provide one anyway to the template.
  //auto label = std::make_shared<Identity<VastNetflow, SamLabel, DestIp>>
  //              (nodeId, featureMap, identifier);
  //consumer->registerConsumer(label);
  //label->registerSubscriber(subscriber, identifier); 

  identifier = "top2";
  int k = 2;
  int N = 10000;
  int b = 1000;
  auto topk = std::make_shared<TopK< EdgeType, DestPort, DestIp>>
                              (N, b, k, nodeId, featureMap, identifier);
                               
  producer->registerConsumer(topk); 

  std::cout << "topk created and registered " << std::endl;
  
  // Five tokens for the 
  // First function token
  auto function1 = [](Feature const * feature)->double {
    int index1 = 0;
    //std::cout << "index1 " << index1 << std::endl;
    //std::cout << "In function1 call " << std::endl;
    //std::cout << feature << std::endl;
    auto topKFeature = static_cast<TopKFeature const *>(feature);
    //std::cout << topKFeature << std::endl;
    //std::cout << &topKFeature->getFrequencies() << std::endl;
    //std::cout << "index1 " << index1 << std::endl;
    return topKFeature->getFrequencies()[index1];    
  };
  auto funcToken1 = std::make_shared<FuncToken<VastNetflow>>(featureMap, 
                                                        function1,
                                                        identifier);

  // Addition token
  auto addOper = std::make_shared<AddOperator<VastNetflow>>(featureMap);

  // Second function token
  auto function2 = [](Feature const * feature)->double {
    int index2 = 1;
    auto topKFeature = static_cast<TopKFeature const *>(feature);
    return topKFeature->getFrequencies()[index2];    
  };
  auto funcToken2 = std::make_shared<FuncToken<VastNetflow>>(featureMap, 
                                                         function2,
                                                         identifier);

  // Lessthan token
  auto greaterThanToken = std::make_shared<GreaterThanOperator<VastNetflow>>(
                        featureMap);
  
  // Number token
  auto numberToken = std::make_shared<NumberToken<VastNetflow>>(featureMap, 
                                                            0.9);

  std::list<std::shared_ptr<ExpressionToken<VastNetflow>>> infixList;
  infixList.push_back(funcToken1);
  infixList.push_back(addOper);
  infixList.push_back(funcToken2);
  infixList.push_back(greaterThanToken);
  infixList.push_back(numberToken);

  auto filterExpression = std::make_shared<Expression<VastNetflow>>(infixList);
    
  auto filter = std::make_shared<Filter<EdgeType, DestIp>>(
    filterExpression, nodeId, featureMap, "servers", queueLength);

  producer->registerConsumer(filter);

  std::cout << "filter created and registiered " << std::endl;

  identifier = "serverSumIncomingFlowSize";
  auto sumIncoming = std::make_shared<ExponentialHistogramSum<size_t, EdgeType,
                                                 SrcTotalBytes,
                                                 DestIp>>
                          (N, 2, nodeId, featureMap, identifier);
  filter->registerConsumer(sumIncoming); 
  sumIncoming->registerSubscriber(subscriber, identifier);
  
  identifier = "serverSumOutgoingFlowSize";
  auto sumOutgoing = std::make_shared<ExponentialHistogramSum<size_t, EdgeType,
                                                  DestTotalBytes,
                                                  DestIp>>
                          (N, 2, nodeId, featureMap, identifier);
  filter->registerConsumer(sumOutgoing);
  sumOutgoing->registerSubscriber(subscriber, identifier);
     
  identifier = "serverVarianceIncomingFlowSize";
  auto varianceIncoming = std::make_shared<ExponentialHistogramVariance<
                               double, EdgeType, SrcTotalBytes, DestIp>>
                          (N, 2, nodeId, featureMap, identifier);
  filter->registerConsumer(varianceIncoming); 
  varianceIncoming->registerSubscriber(subscriber, identifier);
  
  identifier = "serverVarianceOutgoingFlowSize";
  auto varianceOutgoing = std::make_shared<ExponentialHistogramVariance<
                            double, EdgeType, DestTotalBytes, DestIp>>
                              (N, 2, nodeId, featureMap, identifier);
  filter->registerConsumer(varianceOutgoing);
  varianceOutgoing->registerSubscriber(subscriber, identifier);
     
  ////////////////// Creating Time Lapse Series ///////////////////////
  #define DestIp_TimeLapseSeries    1
  #define SrcIp_TimeLapseSeries     2
  #define TimeDiff_TimeLapseSeries  3 
  typedef std::tuple<std::size_t, std::string, std::string, double> 
          TimeLapseSeries;
 
  std::vector<std::shared_ptr<Expression<VastNetflow>>> expressions;
  std::vector<std::string> names;
  std::string name = "TimeLapseSeries_TimeDiff";
  names.push_back(name);
  
  // Expression TimeSeconds - Prev.TimeSeconds
  // 
  // TimeSeconds field token 
  std::shared_ptr<ExpressionToken<VastNetflow>> fieldToken = std::make_shared<
    FieldToken<TimeSeconds, VastNetflow>>(featureMap);
  // Sub operator token
  std::shared_ptr<ExpressionToken<VastNetflow>> subToken = std::make_shared<
    SubOperator<VastNetflow>>(featureMap);
  // Prev.TimeSeconds
  std::shared_ptr<ExpressionToken<VastNetflow>> prevToken = std::make_shared<
    PrevToken<TimeSeconds, VastNetflow>>(featureMap);
    
  std::list<std::shared_ptr<ExpressionToken<VastNetflow>>> infixList2;
  infixList2.push_back(fieldToken);
  infixList2.push_back(subToken);
  infixList2.push_back(prevToken);
    
  auto expression = std::make_shared<Expression<VastNetflow>>(infixList2);
  expressions.push_back(expression); 
   
  auto tupleExpression =std::make_shared<TupleExpression<VastNetflow>>(expressions);
  identifier = "destsrc_timelapseseries";

  auto timeLapseSeries = std::make_shared<TransformProducer<
                     EdgeType, TimeLapseSeries, DestIp, SourceIp>>
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
    std::make_shared<ExponentialHistogramVariance<double, 
      Edge<size_t, EmptyLabel, TimeLapseSeries>,
                           TimeDiff_TimeLapseSeries, 
                           DestIp_TimeLapseSeries, SrcIp_TimeLapseSeries>>
                          (N, 2, nodeId, featureMap, identifier);
  timeLapseSeries->registerConsumer(destSourceTimeDiffVar); 


  identifier = "projectOutSource";
  auto projectToDest = 
    std::make_shared<Project<
      Edge<size_t, EmptyLabel, TimeLapseSeries>, DestIp_TimeLapseSeries, 
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
      sum = sum + feature->evaluate<double>(valueFunc); 
    }
    return sum / myList.size();
  };

  auto destTimeDiffVar = 
    std::make_shared<CollapsedConsumer<EdgeType, DestIp>>(
                                               aveFunction,
                                               "destSourceTimeDiffVariance",
                                               nodeId, 
                                               featureMap, 
                                               identifier); 
  
  filter->registerConsumer(destTimeDiffVar);
  destTimeDiffVar->registerSubscriber(subscriber, identifier);
   
}

int main(int argc, char** argv) {

  /******************* Variables ******************************/

  // There is assumed to be numNodes nodes within the cluster.
  // Each node has name prefix[0,numNodes).  Each of these
  // fields are specified with command line parameters
  int numNodes; //The total number of nodes in the cluster
  int nodeId; //Cardinal number of node within cluster
  string prefix; //Common prefix to all nodes in cluster

  // Ports for communications are allocated over a contiguous 
  // block of numbers, starting at this number
  size_t startingPort;

  // These two parameters relate to how the node receives data
  // Right now we can receive data in two ways, from a socket
  // and a CSV file.  These two are for reading data from a socket.
  // This is an area where SAM can be matured down the road.
  string ncIp; // The ip to read the nc data from.
  size_t ncPort; // The port to read the nc data from.

  // We use ZeroMQ Push/Pull sockets to communicate between
  // nodes.  We found that having multiple sockets to communicate
  // to a single node helped avoid contention.  numSockets
  // sets the total number of push and corresponding pull sockets
  // per node, so in total ((numNodes-1) * numSockets) sockets are.
  // defined per node.  It also helped to have multiple
  // threads to receive the data.  numPullThreads specifie
  // how many threads are used to cover all of the pull sockets
  // for a node.
  size_t numSockets;
  size_t numPullThreads;

  // The timeWindow (s) controls how long intermediate subgraph results
  // are kept.  It should be a little longer than the longest
  // subgraph query
  double timeWindow;
 
  // A small set zeromq communications take forever.  The timeout
  // sets a hard limit on how long for push sockets to wait.
  // The value is in milliseconds
  size_t timeout;

  // The high water mark is a parameter for zeromq communications
  // and it controls how many messages can buffer before being
  // dropped.  This can be set with a SAL preamble statement:
  // HighWaterMark = <int>;
  // This sets a default value in the code that can be overriden
  // with the command-line interface of this generated code.
  size_t hwm;

  // There are two data structures to store the graph for edges
  // local to this node: a compressed sparse row (csr) and 
  // compressed sparse column (csc).  A hash structure is used 
  // to index the edges, either by the source vertex (csr) or the
  // target vertex (csc).  The array for the hash structure is
  // set once and not grown dynamically, though each slot can
  // grow arbitrarily large.  This should be set to a sufficiently
  // large value or much time will be spent linearly probing.
  size_t graphCapacity;

  // Both the SubgraphQueryResultMap (where intermediate subgraphs
  // are stored) and the EdgeRequestMap (where edge requests from
  // other nodes are stored) use a hash structure with an array.
  // The array does not grow dynmically, but each slot can get
  // arbitrarily big.  This should be set to a sufficiently
  // large value or much time will be spent linearly probing.
  // In the future, may want to split these into two different
  // parameters.
  size_t tableCapacity;

  // FeatureMap is another hashtable that needs a capacity
  // specified.  FeatureMap holds all the generated features.
  // FeatureSubscriber is used to create features.  Right now
  // it also needs a capacity specified, but in the future
  // this can likely be removed.  featureCapacity covers both
  // cases.
  size_t featureCapacity;

  // For many of the vertex centric computations we buffer
  // the values and then execute a parallel for loop.
  // queueLength determines the length of the buffer.
  size_t queueLength;

  // inputfile and outputfile are used when we are creating
  // features.  The inputfile has tuples with labels.
  // We then run the pipeline over the inputfile, create features
  // and write the results to the outputfile.
  string inputfile;
  string outputfile;

  // Where subgraph results are written
  string printerLocation;

  /****************** Process commandline arguments ****************/

  po::options_description desc(
    "There are two basic modes supported right now: "
    "1) Running the pipeline against data coming from a socket.\n"
    "2) Running the pipeline against an input file and creating\n"
    " features.\n"
    "These of course should be expanded.  Right now the process\n"
    "allows for creating features on existing data to train\n"
    "offline.  However, using the trained model on live data\n"
    "is currently not supported\n"
    "Allowed options:");
  desc.add_options()
    ("help", "help message")
    ("numNodes",
      po::value<int>(&numNodes)->default_value(1),
      "The number of nodes involved in the computation")
    ("nodeId",
       po::value<int>(&nodeId)->default_value(0),
       "The node id of this node.")
    ("prefix",
      po::value<string>(&prefix)->default_value("node"),
      "The prefix common to all nodes.  The hostnames are formed"
      "by concatenating the prefix with the node id (in"
      "[0, numNodes-1]).  However, when there is only one node"
      "we use localhost.")
    ("startingPort",
       po::value<size_t>(&startingPort)->default_value(10000),
       "The starting port for the zeromq communications")
    ("ncIp",
      po::value<string>(&ncIp)->default_value("localhost"),
      "The ip to receive the data from nc (netcat).  Right now"
      " each node receives data from a socket connection.  "
      "This can be improved in the future.")
    ("ncPort",
      po::value<size_t>(&ncPort)->default_value(9999),
      "The port to receive the data from nc")
    ("numPullThreads",
      po::value<size_t>(&numPullThreads)->default_value(1),
      "Number of pull threads (default 1)")
    ("numSockets",
      po::value<size_t>(&numSockets)->default_value(1),
      "Number of push sockets a node creates to talk to another"
      "node (default 1)")
    ("timeWindow",
      po::value<double>(&timeWindow)->default_value(10),
      "How long in seconds to keep intermediate results around")
    ("timeout",
      po::value<size_t>(&timeout)->default_value(1000),
      "How long in milliseconds to wait before giving up on push"
      "socket send")
    ("graphCapacity",
      po::value<size_t>(&graphCapacity)->default_value(100000),
      "How many slots in the csr and csc (default: 100000).")
    ("tableCapacity",
      po::value<size_t>(&tableCapacity)->default_value(1000),
      "How many slots in SubgraphQueryResultMap and EdgeRequestMap"
      " (default 1000).")
    ("featureCapacity",
      po::value<size_t>(&featureCapacity)->default_value(10000),
      "The capacity of the FeatureMap and FeatureSubcriber")
    ("hwm",
      po::value<size_t>(&hwm)->default_value(10000),
      "The high water mark (how many items can queue up before we"
      "start dropping")
    ("queueLength",
      po::value<size_t>(&queueLength)->default_value(1000),
      "We fill a queue before sending things in parallel to all"
      " consumers.  This controls the size of that queue.")
    ("create_features",
      "If specified, will read tuples from --inputfile and"
      " output to --outputfile a csv feature file")
    ("inputfile",
      po::value<string>(&inputfile),
      "If --create_features is specified, the input should be"
      " a file with labeled tuples.")
    ("outputfile",
      po::value<string>(&outputfile),
      "If --create_features is specified, the produced file will"
      " be a csv file of features.")
    ("printerLocation",
      po::value<std::string>(&printerLocation)->default_value(""),
      "Where subgraph results are written.")
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

  vector<string> hostnames(numNodes); // A vector of hosts in the cluster
  vector<std::size_t> ports(numNodes); // Vector of ports to use in the cluster

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

  // The global featureMap (global for all features generated for this node;
  // each node has it's own featuremap.
  std::cout << "About to create feature Map " << std::endl;
  auto featureMap = std::make_shared<FeatureMap>(featureCapacity);

  /********************** Creating features ******************************/
  if (vm.count("create_features")) 
  {
    if (inputfile == "") {
      std::cout << "--create_features was specified but no input file"
                << " was listed with --inputfile." << std::endl;
      return -1; 
    }
    if (outputfile == "") {
      std::cout << "--create_features was specified but no output file"
                << " was listed with --outputfile." << std::endl;
      return -1; 
    }

    typedef VastNetflow TupleType;
    typedef SingleBoolLabel LabelType;
    typedef Edge<size_t, LabelType, TupleType> EdgeType;
    typedef TupleStringHashFunction<TupleType, SourceIp> SourceHash;
    typedef TupleStringHashFunction<TupleType, DestIp> TargetHash;
    typedef TuplizerFunction<EdgeType, MakeVastNetflow> Tuplizer;
    typedef ZeroMQPushPull<EdgeType, Tuplizer,
                           SourceHash, TargetHash>
                           PartitionType;
    typedef ReadCSV<EdgeType, Tuplizer> ReadCSVType;
    typedef BaseProducer<EdgeType> ProducerType;

    // We read the netflow data from a file.  It assumes each netflow 
    // has a label at the beginning.
    auto receiver = std::make_shared<ReadCSVType>(nodeId, inputfile);

    // subscriber collects the features for each netflow
    auto subscriber = std::make_shared<FeatureSubscriber>(outputfile, 
                                                          featureCapacity);

    std::cout << "Creating Pipeline " << std::endl;
    // createPipeline creates all the operators and ties them together.  It 
    // also notifies the designated feature producers of the subscriber.
    /*createPipeline(receiver, featureMap, subscriber, NULL,
                   queueLength,
                   numNodes,
                   nodeId,
                   hostnames,
                   ports,
                   hwm,
                   N, b, k);
    */
    std::cout << "Created Pipeline " << std::endl;
    
    // You must call init before starting the pipeline.
    subscriber->init();
    
    // Connects the receiver to the input data but doesn't start ingestion.
    if (!receiver->connect()) {
      std::cout << "Problems opening file " << inputfile << std::endl;
      return -1;
    }
    
    milliseconds ms1 = duration_cast<milliseconds>(
      system_clock::now().time_since_epoch()
    );
    // Starts the pipeline
    receiver->receive();
    milliseconds ms2 = duration_cast<milliseconds>(
      system_clock::now().time_since_epoch()
    );
    std::cout << "Seconds for Node" << nodeId << ": "  
      << static_cast<double>(ms2.count() - ms1.count()) / 1000 << std::endl;
    
    std::cout << "Finished" << std::endl;
    return 0;
    
  } 
  /******************* Running pipeline without model ******************/
  else 
  {
    typedef VastNetflow TupleType;
    typedef EmptyLabel LabelType;
    typedef Edge<size_t, LabelType, TupleType> EdgeType;
    typedef TupleStringHashFunction<TupleType, SourceIp> SourceHash;
    typedef TupleStringHashFunction<TupleType, DestIp> TargetHash;
    typedef TuplizerFunction<EdgeType, MakeVastNetflow> Tuplizer;
    typedef ZeroMQPushPull<EdgeType, Tuplizer, 
                           SourceHash, TargetHash>
                           PartitionType;
    typedef ReadSocket<EdgeType, Tuplizer> ReadSocketType; 
    typedef BaseProducer<EdgeType> ProducerType;

    ReadSocketType receiver(nodeId, ncIp, ncPort);

    size_t timeout = 1000;

    // Creating the ZeroMQPushPull consumer.  This consumer is responsible for
    // getting the data from the receiver (e.g. a socket or a file) and then
    // publishing it in a load-balanced way to the cluster.
    auto consumer = std::make_shared<PartitionType>(queueLength,
                                   numNodes, 
                                   nodeId, 
                                   hostnames, 
                                   startingPort, timeout, false,
                                   hwm);

    receiver.registerConsumer(consumer);

    //createPipeline(consumer, featureMap, featureNames, nodeId);

    if (!receiver.connect()) {
      std::cout << "Couldn't connected to " << ncIp << ":" << ncPort << std::endl;
      return -1;
    }

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
}

