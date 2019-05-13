/*
 * SimpleFeatures.cpp
 * This creates features on the netflow features without any
 * operators added in.
 * The base netflow features are:
 * 1) duration
 * 2) source app bytes
 * 3) dest app bytes
 * 4) source total bytes
 * 5) dest total bytes
 * 6) source packets
 * 7) dest packets 
 * 
 * The types of features created are
 * 1) average 
 * 2) variance
 *  Created on: March 15, 2017
 *      Author: elgood
 */

#include <string>
#include <vector>
#include <stdlib.h>
#include <iostream>
#include <chrono>

#include <boost/program_options.hpp>

#include <sam/ReadSocket.hpp>
#include <sam/ReadCSV.hpp>
#include <sam/ZeroMQPushPull.hpp>
#include <sam/TopK.hpp>
#include <sam/Expression.hpp>
#include <sam/TupleExpression.hpp>
#include <sam/Filter.hpp>
#include <sam/ExponentialHistogramSum.hpp>
#include <sam/ExponentialHistogramVariance.hpp>
#include <sam/Netflow.hpp>
#include <sam/TransformProducer.hpp>
#include <sam/Project.hpp>
#include <sam/CollapsedConsumer.hpp>
#include <sam/Identity.hpp>

using std::string;
using std::vector;
using std::cout;
using std::endl;

namespace po = boost::program_options;

using namespace sam;
using namespace std::chrono;

typedef TupleStringHashFunction<Netflow, SourceIp> SourceHash;
typedef TupleStringHashFunction<Netflow, DestIp> TargetHash;
typedef ZeroMQPushPull<Netflow, NetflowTuplizer, SourceHash, TargetHash>
        PartitionType;

//zmq::context_t context(1);

void createPipeline(
                 std::shared_ptr<ReadCSV> readCSV,
                 std::shared_ptr<FeatureMap> featureMap,
                 std::shared_ptr<FeatureSubscriber> subscriber,
                 std::shared_ptr<PartitionType> pushpull,
                 std::size_t queueLength,
                 std::size_t numNodes,
                 std::size_t nodeId,
                 std::vector<std::string> const& hostnames,
                 std::vector<std::size_t> const& ports,
                 std::size_t hwm,
                 std::size_t N,
                 std::size_t b,
                 std::size_t k)
{
  // An operator to get the label from each netflow and add it to the
  // subscriber.
  string identifier = "label";

  // Doesn't really need a key, but provide one anyway to the template.
  auto label = std::make_shared<Identity<Netflow, SamLabel, DestIp>>
                (nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(label);
  } else {
    pushpull->registerConsumer(label);
  }
  if (subscriber != NULL) {
    label->registerSubscriber(subscriber, identifier); 
  }


  /** Dest Ip as key **/
  identifier = "averageSrcTotalBytes";
  auto averageSrcTotalBytes = std::make_shared<
                      ExponentialHistogramAve<double, Netflow,
                                                 SrcTotalBytes,
                                                 DestIp>>
                          (N, 2, nodeId, featureMap, identifier);

  if (readCSV != NULL) {
    readCSV->registerConsumer(averageSrcTotalBytes);
  } else {
    pushpull->registerConsumer(averageSrcTotalBytes);
  }
  if (subscriber != NULL) {
    averageSrcTotalBytes->registerSubscriber(subscriber, identifier);
  }

  identifier = "varSrcTotalBytes";
  auto varSrcTotalBytes = std::make_shared<
                      ExponentialHistogramVariance<double, Netflow,
                                                 SrcTotalBytes,
                                                 DestIp>>
                          (N, 2, nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(varSrcTotalBytes);
  } else {
    pushpull->registerConsumer(varSrcTotalBytes);
  }
  if (subscriber != NULL) {
    varSrcTotalBytes->registerSubscriber(subscriber, identifier);
  }

  identifier = "averageDestTotalBytes";
  auto averageDestTotalBytes = std::make_shared<
                      ExponentialHistogramAve<double, Netflow,
                                                 DestTotalBytes,
                                                 DestIp>>
                          (N, 2, nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(averageDestTotalBytes);
  } else {
    pushpull->registerConsumer(averageDestTotalBytes);
  }
  if (subscriber != NULL) {
    averageDestTotalBytes->registerSubscriber(subscriber, identifier);
  }

  identifier = "varDestTotalBytes";
  auto varDestTotalBytes = std::make_shared<
                      ExponentialHistogramVariance<double, Netflow,
                                                 DestTotalBytes,
                                                 DestIp>>
                          (N, 2, nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(varDestTotalBytes);
  } else {
    pushpull->registerConsumer(varDestTotalBytes);
  }
  if (subscriber != NULL) {
    varDestTotalBytes->registerSubscriber(subscriber, identifier);
  }

  identifier = "averageDuration";
  auto averageDuration = std::make_shared<
                      ExponentialHistogramAve<double, Netflow,
                                                 DurationSeconds,
                                                 DestIp>>
                          (N, 2, nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(averageDuration);
  } else {
    pushpull->registerConsumer(averageDuration);
  }
  if (subscriber != NULL) {
    averageDuration->registerSubscriber(subscriber, identifier);
  }

  identifier = "varDuration";
  auto varDuration = std::make_shared<
                      ExponentialHistogramVariance<double, Netflow,
                                                 DurationSeconds,
                                                 DestIp>>
                          (N, 2, nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(varDuration);
  } else {
    pushpull->registerConsumer(varDuration);
  }
  if (subscriber != NULL) {
    varDuration->registerSubscriber(subscriber, identifier);
  }

  identifier = "averageSrcPayloadBytes";
  auto averageSrcPayloadBytes = std::make_shared<
                      ExponentialHistogramAve<double, Netflow,
                                                 SrcPayloadBytes,
                                                 DestIp>>
                          (N, 2, nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(averageSrcPayloadBytes);
  } else {
    pushpull->registerConsumer(averageSrcPayloadBytes);
  }
  if (subscriber != NULL) {
    averageSrcPayloadBytes->registerSubscriber(subscriber, identifier);
  }

  identifier = "varSrcPayloadBytes";
  auto varSrcPayloadBytes = std::make_shared<
                      ExponentialHistogramVariance<double, Netflow,
                                                 SrcPayloadBytes,
                                                 DestIp>>
                          (N, 2, nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(varSrcPayloadBytes);
  } else {
    pushpull->registerConsumer(varSrcPayloadBytes);
  }
  if (subscriber != NULL) {
    varSrcPayloadBytes->registerSubscriber(subscriber, identifier);
  }

  identifier = "averageDestPayloadBytes";
  auto averageDestPayloadBytes = std::make_shared<
                      ExponentialHistogramAve<double, Netflow,
                                                 DestPayloadBytes,
                                                 DestIp>>
                          (N, 2, nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(averageDestPayloadBytes);
  } else {
    pushpull->registerConsumer(averageDestPayloadBytes);
  }
  if (subscriber != NULL) {
    averageDestPayloadBytes->registerSubscriber(subscriber, identifier);
  }

  identifier = "varDestPayloadBytes";
  auto varDestPayloadBytes = std::make_shared<
                      ExponentialHistogramVariance<double, Netflow,
                                                 DestPayloadBytes,
                                                 DestIp>>
                          (N, 2, nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(varDestPayloadBytes);
  } else {
    pushpull->registerConsumer(varDestPayloadBytes);
  }
  if (subscriber != NULL) {
    varDestPayloadBytes->registerSubscriber(subscriber, identifier);
  }

  identifier = "averageSrcPacketCount";
  auto averageSrcPacketCount = std::make_shared<
                      ExponentialHistogramAve<double, Netflow,
                                                 FirstSeenSrcPacketCount,
                                                 DestIp>>
                          (N, 2, nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(averageSrcPacketCount);
  } else {
    pushpull->registerConsumer(averageSrcPacketCount);
  }
  if (subscriber != NULL) {
    averageSrcPacketCount->registerSubscriber(subscriber, identifier);
  }

  identifier = "varSrcPacketCount";
  auto varSrcPacketCount = std::make_shared<
                      ExponentialHistogramVariance<double, Netflow,
                                                 FirstSeenSrcPacketCount,
                                                 DestIp>>
                          (N, 2, nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(varSrcPacketCount);
  } else {
    pushpull->registerConsumer(varSrcPacketCount);
  }
  if (subscriber != NULL) {
    varSrcPacketCount->registerSubscriber(subscriber, identifier);
  }

  identifier = "averageDestPacketCount";
  auto averageDestPacketCount = std::make_shared<
                      ExponentialHistogramAve<double, Netflow,
                                                 FirstSeenDestPacketCount,
                                                 DestIp>>
                          (N, 2, nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(averageDestPacketCount);
  } else {
    pushpull->registerConsumer(averageDestPacketCount);
  }
  if (subscriber != NULL) {
    averageDestPacketCount->registerSubscriber(subscriber, identifier);
  }

  identifier = "varDestPacketCount";
  auto varDestPacketCount = std::make_shared<
                      ExponentialHistogramVariance<double, Netflow,
                                                 FirstSeenDestPacketCount,
                                                 DestIp>>
                          (N, 2, nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(varDestPacketCount);
  } else {
    pushpull->registerConsumer(varDestPacketCount);
  }
  if (subscriber != NULL) {
    varDestPacketCount->registerSubscriber(subscriber, identifier);
  }

  /** SourceIp as key **/
  identifier = "averageSrcTotalBytesSourceIp";
  auto averageSrcTotalBytesSourceIp = std::make_shared<
                      ExponentialHistogramAve<double, Netflow,
                                                 SrcTotalBytes,
                                                 SourceIp>>
                          (N, 2, nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(averageSrcTotalBytesSourceIp);
  } else {
    pushpull->registerConsumer(averageSrcTotalBytesSourceIp);
  }
  if (subscriber != NULL) {
    averageSrcTotalBytesSourceIp->registerSubscriber(subscriber, identifier);
  }

  identifier = "varSrcTotalBytesSourceIp";
  auto varSrcTotalBytesSourceIp = std::make_shared<
                      ExponentialHistogramVariance<double, Netflow,
                                                 SrcTotalBytes,
                                                 SourceIp>>
                          (N, 2, nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(varSrcTotalBytesSourceIp);
  } else {
    pushpull->registerConsumer(varSrcTotalBytesSourceIp);
  }
  if (subscriber != NULL) {
    varSrcTotalBytesSourceIp->registerSubscriber(subscriber, identifier);
  }

  identifier = "averageDestTotalBytesSourceIp";
  auto averageDestTotalBytesSourceIp = std::make_shared<
                      ExponentialHistogramAve<double, Netflow,
                                                 DestTotalBytes,
                                                 SourceIp>>
                          (N, 2, nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(averageDestTotalBytesSourceIp);
  } else {
    pushpull->registerConsumer(averageDestTotalBytesSourceIp);
  }
  if (subscriber != NULL) {
    averageDestTotalBytesSourceIp->registerSubscriber(subscriber, identifier);
  }

  identifier = "varDestTotalBytesSourceIp";
  auto varDestTotalBytesSourceIp = std::make_shared<
                      ExponentialHistogramVariance<double, Netflow,
                                                 DestTotalBytes,
                                                 SourceIp>>
                          (N, 2, nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(varDestTotalBytesSourceIp);
  } else {
    pushpull->registerConsumer(varDestTotalBytesSourceIp);
  }
  if (subscriber != NULL) {
    varDestTotalBytesSourceIp->registerSubscriber(subscriber, identifier);
  }

  identifier = "averageDurationSourceIp";
  auto averageDurationSourceIp = std::make_shared<
                      ExponentialHistogramAve<double, Netflow,
                                                 DurationSeconds,
                                                 SourceIp>>
                          (N, 2, nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(averageDurationSourceIp);
  } else {
    pushpull->registerConsumer(averageDurationSourceIp);
  }
  if (subscriber != NULL) {
    averageDurationSourceIp->registerSubscriber(subscriber, identifier);
  }

  identifier = "varDurationSourceIp";
  auto varDurationSourceIp = std::make_shared<
                      ExponentialHistogramVariance<double, Netflow,
                                                 DurationSeconds,
                                                 SourceIp>>
                          (N, 2, nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(varDurationSourceIp);
  } else {
    pushpull->registerConsumer(varDurationSourceIp);
  }
  if (subscriber != NULL) {
    varDurationSourceIp->registerSubscriber(subscriber, identifier);
  }

  identifier = "averageSrcPayloadBytesSourceIp";
  auto averageSrcPayloadBytesSourceIp = std::make_shared<
                      ExponentialHistogramAve<double, Netflow,
                                                 SrcPayloadBytes,
                                                 SourceIp>>
                          (N, 2, nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(averageSrcPayloadBytesSourceIp);
  } else {
    pushpull->registerConsumer(averageSrcPayloadBytesSourceIp);
  }
  if (subscriber != NULL) {
    averageSrcPayloadBytesSourceIp->registerSubscriber(subscriber, identifier);
  }

  identifier = "varSrcPayloadBytesSourceIp";
  auto varSrcPayloadBytesSourceIp = std::make_shared<
                      ExponentialHistogramVariance<double, Netflow,
                                                 SrcPayloadBytes,
                                                 SourceIp>>
                          (N, 2, nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(varSrcPayloadBytesSourceIp);
  } else {
    pushpull->registerConsumer(varSrcPayloadBytesSourceIp);
  }
  if (subscriber != NULL) {
    varSrcPayloadBytesSourceIp->registerSubscriber(subscriber, identifier);
  }

  identifier = "averageDestPayloadBytesSourceIp";
  auto averageDestPayloadBytesSourceIp = std::make_shared<
                      ExponentialHistogramAve<double, Netflow,
                                                 DestPayloadBytes,
                                                 SourceIp>>
                          (N, 2, nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(averageDestPayloadBytesSourceIp);
  } else {
    pushpull->registerConsumer(averageDestPayloadBytesSourceIp);
  }
  if (subscriber != NULL) {
    averageDestPayloadBytesSourceIp->registerSubscriber(subscriber, identifier);
  }

  identifier = "varDestPayloadBytesSourceIp";
  auto varDestPayloadBytesSourceIp = std::make_shared<
                      ExponentialHistogramVariance<double, Netflow,
                                                 DestPayloadBytes,
                                                 SourceIp>>
                          (N, 2, nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(varDestPayloadBytesSourceIp);
  } else {
    pushpull->registerConsumer(varDestPayloadBytesSourceIp);
  }
  if (subscriber != NULL) {
    varDestPayloadBytesSourceIp->registerSubscriber(subscriber, identifier);
  }

  identifier = "averageSrcPacketCountSourceIp";
  auto averageSrcPacketCountSourceIp = std::make_shared<
                      ExponentialHistogramAve<double, Netflow,
                                                 FirstSeenSrcPacketCount,
                                                 SourceIp>>
                          (N, 2, nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(averageSrcPacketCountSourceIp);
  } else {
    pushpull->registerConsumer(averageSrcPacketCountSourceIp);
  }
  if (subscriber != NULL) {
    averageSrcPacketCountSourceIp->registerSubscriber(subscriber, identifier);
  }

  identifier = "varSrcPacketCountSourceIp";
  auto varSrcPacketCountSourceIp = std::make_shared<
                      ExponentialHistogramVariance<double, Netflow,
                                                 FirstSeenSrcPacketCount,
                                                 SourceIp>>
                          (N, 2, nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(varSrcPacketCountSourceIp);
  } else {
    pushpull->registerConsumer(varSrcPacketCountSourceIp);
  }
  if (subscriber != NULL) {
    varSrcPacketCountSourceIp->registerSubscriber(subscriber, identifier);
  }

  identifier = "averageDestPacketCountSourceIp";
  auto averageDestPacketCountSourceIp = std::make_shared<
                      ExponentialHistogramAve<double, Netflow,
                                                 FirstSeenDestPacketCount,
                                                 SourceIp>>
                          (N, 2, nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(averageDestPacketCountSourceIp);
  } else {
    pushpull->registerConsumer(averageDestPacketCountSourceIp);
  }
  if (subscriber != NULL) {
    averageDestPacketCountSourceIp->registerSubscriber(subscriber, identifier);
  }

  identifier = "varDestPacketCountSourceIp";
  auto varDestPacketCountSourceIp = std::make_shared<
                      ExponentialHistogramVariance<double, Netflow,
                                                 FirstSeenDestPacketCount,
                                                 SourceIp>>
                          (N, 2, nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(varDestPacketCountSourceIp);
  } else {
    pushpull->registerConsumer(varDestPacketCountSourceIp);
  }
  if (subscriber != NULL) {
    varDestPacketCountSourceIp->registerSubscriber(subscriber, identifier);
  }

}

int main(int argc, char** argv) {

  string ip; ///> The ip to read the nc data from.
  std::size_t ncPort; ///> The port to read the nc data from.
  std::size_t numNodes; ///> The number of nodes in the cluster
  std::size_t nodeId; ///> The node id of this node
  string prefix; ///> The prefix to the nodes
  std::size_t startingPort; ///> The starting port number
  std::size_t hwm; ///> The high-water mark (zeromq parameter)
  std::size_t queueLength; ///> The length of the input queue
  std::size_t N; ///> The total number of elements in a sliding window
  std::size_t b; ///> The number of elements in a dormant or active window
  std::size_t k; ///> The number of elements to keep track of
  std::size_t nop; //not used
  string inputfile = "";
  string outputfile = "";
  std::size_t capacity = 10000;////> Capacity of FeatureMap and subscriber

  // The training data if learning the classifier
  //arma::mat trainingData;

  // The model that can be trained from example data or if a trained model
  // exists, can be loaded from the filesystem.
  //NBCModel model;

  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "help message")
    ("ip", po::value<string>(&ip)->default_value("localhost"), 
      "The ip to receive the data from nc")
    ("ncPort", po::value<std::size_t>(&ncPort)->default_value(9999), 
      "The port to receive the data from nc")
    ("numNodes", po::value<std::size_t>(&numNodes)->default_value(1), 
      "The number of nodes involved in the computation")
    ("nodeId", po::value<std::size_t>(&nodeId)->default_value(0), 
      "The node id of this node")
    ("prefix", po::value<string>(&prefix)->default_value("node"), 
      "The prefix common to all nodes")
    ("startingPort", po::value<std::size_t>(&startingPort)->default_value(
      10000),  "The starting port for the zeromq communications")
    ("hwm", po::value<std::size_t>(&hwm)->default_value(10000), 
      "The high water mark (how many items can queue up before we start "
      "dropping)")
    ("queueLength", po::value<std::size_t>(&queueLength)->default_value(10000),
      "We fill a queue before sending things in parallel to all consumers."
      "  This controls the size of that queue.")
    ("N", po::value<std::size_t>(&N)->default_value(10000),
      "The total number of elements in a sliding window")
    ("b", po::value<std::size_t>(&b)->default_value(1000),
      "The number of elements per block (active or dynamic window)")
    ("nop", po::value<std::size_t>(&nop)->default_value(1),
      "The number of simultaneous operators")
    ("create_features", "If specified, will read a netflow feature file "
     "from --inputfile and output to --outputfile a csv feature file")
    ("train", "If specified, will read a csv feature file from --inputfile"
     " and output to --outputfile a learned model.")
    ("test", "If specified, will read a learned model from --inputfile"
     " and apply it to the data.")
    ("inputfile", po::value<string>(&inputfile),
      "If --create_features is specified, the input should be a file with"
      " netflow.  If --train is specified, the input should be a csv file"
      " of features (the output of --create_features).  If --test is specified,"
      " the input should be a model (the output of --train).")
    ("outputfile", po::value<string>(&outputfile),
      "If --create_features is specified, the produced file will be a csv"
      " file of features.  If --train is specified, the produced file will be"
      " a learned model.")
    ("capacity", po::value<std::size_t>(&capacity)->default_value(10000),
      "The capacity of the FeatureMap and FeatureSubcriber")
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
  auto featureMap = std::make_shared<FeatureMap>(capacity);

  
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
    
    // We read the netflow data from a file.  It assumes each netflow 
    // has a label at the beginning.
    auto receiver = std::make_shared<ReadCSV>(inputfile);

    // subscriber collects the features for each netflow
    auto subscriber = std::make_shared<FeatureSubscriber>(outputfile, capacity);

    std::cout << "Creating Pipeline " << std::endl;
    // createPipeline creates all the operators and ties them together.  It 
    // also notifies the designated feature producers of the subscriber.
    createPipeline(receiver, featureMap, subscriber, NULL, 
                   queueLength,
                   numNodes,
                   nodeId,
                   hostnames,
                   ports,
                   hwm,
                   N, b, k);
   
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
  /********************* Learning Model *********************************/
  else if (vm.count("train"))
  {
    /*if (inputfile == "") {
      std::cout << "--train was specified but no input file"
                << " was listed with --inputfile." << std::endl;
      return -1; 
    }
    if (outputfile == "") {
      std::cout << "--train was specified but no output file"
                << " was listed with --outputfile." << std::endl;
      return -1; 
    }

    // The true parameter transposes the data.  In mlpack, rows are features 
    // and columns are observations, which makes things confusing.
    data::Load(inputfile, trainingData, true);

    arma::Row<double> labels = trainingData.row(0);

    //data::NormalizeLabels(trainingData.row(0), labels, model.mappings);
    
    // Remove the label row
    trainingData.shed_row(0);

    Timer::Start("nbc_training");
    std::cout << "About to train " << std::endl;
    model.nbc = NaiveBayesClassifier<>(trainingData, labels,
      model.mappings.n_elem, true);

    data::Save(outputfile, "model", model, true);
    std::cout << "Saved Model " << std::endl;
    Timer::Stop("nbc_training");
    return 0;
    */
  } 
  /******************** Applying model *********************************/
  else if (vm.count("test"))
  {
    /*if (inputfile == "") {
      std::cout << "--test was specified but no input file"
                << " was listed with --inputfile." << std::endl;
      return -1; 
    }
    data::Load(inputfile, "model", model);
    cout << "model.mappings " << model.mappings << std::endl;
    */
  }
  /******************* Running pipeline without model ******************/
  else 
  {

    auto receiver = std::make_shared<ReadSocket>(ip, ncPort);

    // Make a commandline argument
    size_t timeout = 1000;

    // Creating the ZeroMQPushPull consumer.  This consumer is responsible for
    // getting the data from the receiver (e.g. a socket or a file) and then
    // publishing it in a load-balanced way to the cluster.
    auto consumer = std::make_shared<PartitionType>(queueLength,
                                   numNodes, 
                                   nodeId, 
                                   hostnames, 
                                   //ports, 
                                   startingPort, timeout, false,
                                   hwm);

    receiver->registerConsumer(consumer);

    createPipeline(NULL, featureMap, NULL, consumer,
                   queueLength,
                   numNodes,
                   nodeId,
                   hostnames,
                   ports,
                   hwm,
                   N, b, k);
 
    if (!receiver->connect()) {
      std::cout << "Couldn't connected to " << ip << ":" << ncPort << std::endl;
      return -1;
    }

    milliseconds ms1 = duration_cast<milliseconds>(
      system_clock::now().time_since_epoch()
    );
    receiver->receive();
    milliseconds ms2 = duration_cast<milliseconds>(
      system_clock::now().time_since_epoch()
    );
    std::cout << "Seconds for Node" << nodeId << ": "  
      << static_cast<double>(ms2.count() - ms1.count()) / 1000 << std::endl;
  }
}
  
 

