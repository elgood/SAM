/*
 * Servers.cpp
 * This does the server query as described in Disclosure.
 *
 *  Created on: March 15, 2017
 *      Author: elgood
 */

#include <string>
#include <vector>
#include <list>
#include <stdlib.h>
#include <iostream>
#include <chrono>

#include <boost/program_options.hpp>

//#include <mlpack/core.hpp>
//#include <mlpack/methods/naive_bayes/naive_bayes_classifier.hpp>

#include "ReadSocket.hpp"
#include "ReadCSV.hpp"
#include "ZeroMQPushPull.hpp"
#include "TopK.hpp"
#include "Expression.hpp"
#include "Filter.hpp"
#include "Netflow.hpp"
//#include "Learning.hpp"
#include "Identity.hpp"

#define DEBUG 1

using std::string;
using std::vector;
using std::cout;
using std::endl;

namespace po = boost::program_options;

using namespace sam;
using namespace std::chrono;
//using namespace mlpack;
//using namespace mlpack::naive_bayes;

void createPipeline(
                 std::shared_ptr<ReadCSV> readCSV,
                 std::shared_ptr<FeatureMap> featureMap,
                 std::shared_ptr<FeatureSubscriber> subscriber,
                 std::shared_ptr<ZeroMQPushPull> pushpull,
                 std::size_t queueLength,
                 std::size_t numNodes,
                 std::size_t nodeId,
                 std::vector<std::string> const& hostnames,
                 std::vector<std::size_t> const& ports,
                 std::size_t hwm)
{

  // An operator to get the label from each netflow and add it to the
  // subscriber.
  string identifier = "label";

  // Doesn't really need a key, but provide one anyway to the template.
  auto label = std::make_shared<Identity<Netflow, Label, DestIp>>
                (nodeId, featureMap, identifier);
  if (readCSV != NULL) {
    readCSV->registerConsumer(label);
  } else {
    pushpull->registerConsumer(label);
  }
  if (subscriber != NULL) {
    label->registerSubscriber(subscriber, identifier); 
  }

  identifier = "top2";
  int k = 2;
  int N = 10000;
  int b = 1000;
  auto topk = std::make_shared<TopK<size_t, Netflow, 
                               DestPort, DestIp>>(
                               N, b, k, nodeId,
                               featureMap, identifier);
  
  if (readCSV != NULL) {
    readCSV->registerConsumer(topk);
  } else {
    pushpull->registerConsumer(topk);
  }
  if (subscriber != NULL) {
    topk->registerSubscriber(subscriber, identifier);
  }

  // Five tokens for the 
  // First function token
  auto function1 = [](Feature const * feature)->double {
    auto topKFeature = static_cast<TopKFeature const *>(feature);
    return topKFeature->getFrequencies()[0];    
  };
  auto funcToken1 = std::make_shared<FuncToken<Netflow>>(featureMap, 
                                                    function1, identifier);

  // Addition token
  auto addOper = std::make_shared<AddOperator<Netflow>>(featureMap);

  // Second function token
  auto function2 = [](Feature const * feature)->double {
    auto topKFeature = static_cast<TopKFeature const *>(feature);
    return topKFeature->getFrequencies()[1];    
  };

  auto funcToken2 = std::make_shared<FuncToken<Netflow>>(featureMap, 
                                                    function2, identifier);

  // Lessthan token
  auto lessThanToken =std::make_shared<LessThanOperator<Netflow>>(featureMap);
  
  // Number token
  auto numberToken = std::make_shared<NumberToken<Netflow>>(featureMap, 0.9);

  auto infixList = std::make_shared<std::list<std::shared_ptr<
                    ExpressionToken<Netflow>>>>();
  infixList->push_back(funcToken1);
  infixList->push_back(addOper);
  infixList->push_back(funcToken2);
  infixList->push_back(lessThanToken);
  infixList->push_back(numberToken);

  auto filterExpression = std::make_shared<Expression<Netflow>>(*infixList);
   
  auto filter = std::make_shared<Filter<Netflow, DestIp>>(
    filterExpression, nodeId, featureMap, "servers", queueLength);
  if (readCSV != NULL) {
    readCSV->registerConsumer(filter);
  } else {
    pushpull->registerConsumer(filter);
  }
}

int main(int argc, char** argv) {
  
  string ip; ///> The ip to read the nc data from.
  std::size_t ncPort; ///> The port to read the nc data from.
  std::size_t numNodes; ///> The number of nodes in the cluster
  std::size_t nodeId; ///> The node id of this node
  string prefix; ///> The prefix to the nodes
  std::size_t startingPort; ///> The starting port number for push/pull sockets
  std::size_t hwm; ///> The high-water mark (Zeromq parameter)
  std::size_t queueLength; ///> The length of the input queue
  std::size_t N; ///> The total number of elements in a sliding window
  std::size_t b; ///> The number of elements in a dormant or active window
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
      10000), "The starting port for the zeromq communications")
    ("hwm", po::value<std::size_t>(&hwm)->default_value(10000), 
      "The high water mark (how many items can queue up before we start "
      "dropping)")
    ("queueLength", po::value<std::size_t>(&queueLength)->default_value(1000),
      "We fill a queue before sending things in parallel to all consumers."
      "  This controls the size of that queue.")
    ("nop", po::value<std::size_t>(&nop)->default_value(1),
      "The number of simultaneous operators")
    ("create_features", "If specified, will read a netflow feature file "
     "from --inputfile and output to --outputfile a csv feature file")
    ("train", "If specified, will read a csv feature file from --inputfile"
     " and output to --outputfile a learned model.")
    ("test", "If specified, will read a learned model from --inputfile"
     " and apply it to the data.")
    ("inputfile", po::value<std::string>(&inputfile),
      "If --create_features is specified, the input should be a file with"
      " netflow.  If --train is specified, the input should be a csv file"
      " of features (the output of --create_features).  If --test is specified,"
      " the input should be a model (the output of --train).")
    ("outputfile", po::value<std::string>(&outputfile),
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

    // createPipeline creates all the operators and ties them together.  It 
    // also notifies the designated feature producers of the subscriber.
    createPipeline(receiver, featureMap, subscriber, NULL, 
                   queueLength,
                   numNodes,
                   nodeId,
                   hostnames,
                   ports,
                   hwm);
    
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

    arma::Row<size_t> labels;

    data::NormalizeLabels(trainingData.row(0), labels, model.mappings);
    
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

  }
  /******************* Running pipeline without model ******************/
  else 
  {

    ReadSocket receiver(ip, ncPort);

    // Creating the ZeroMQPushPull consumer.  This consumer is responsible for
    // getting the data from the receiver (e.g. a socket or a file) and then
    // publishing it in a load-balanced way to the cluster.
    auto consumer = std::make_shared<ZeroMQPushPull>(queueLength,
                                   numNodes, 
                                   nodeId, 
                                   hostnames, 
                                   ports, 
                                   hwm);

    receiver.registerConsumer(consumer);


    //createPipeline(consumer, featureMap, featureNames, nodeId);

    if (!receiver.connect()) {
      std::cout << "Couldn't connected to " << ip << ":" << ncPort << std::endl;
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

