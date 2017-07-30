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

#include <mlpack/core.hpp>
#include <mlpack/methods/naive_bayes/naive_bayes_classifier.hpp>

#include "ReadSocket.h"
#include "ReadCSV.hpp"
#include "ZeroMQPushPull.hpp"
#include "TopK.hpp"
#include "Expression.hpp"
#include "Filter.hpp"
#include "Netflow.hpp"
#include "Learning.hpp"

#define DEBUG 1

using std::string;
using std::vector;
using std::cout;
using std::endl;

namespace po = boost::program_options;

using namespace sam;
using namespace std::chrono;
using namespace mlpack;
using namespace mlpack::naive_bayes;

void createPipeline(std::shared_ptr<ZeroMQPushPull> consumer,
                 FeatureMap& featureMap,
                 std::shared_ptr<FeatureSubscriber> subscriber,
                 int nodeId)
{
    string identifier = "top2";
    int k = 2;
    int N = 10000;
    int b = 1000;
    auto topk = std::make_shared<TopK<size_t, Netflow, 
                                 DestPort, DestIp>>(
                                 N, b, k, nodeId,
                                 featureMap, identifier);
    consumer->registerConsumer(topk); 
    topk->registerSubscriber(subscriber, identifier);

    // Five tokens for the 
    // First function token
    int index1 = 0;
    auto function1 = [&index1](Feature const * feature)->double {
      auto topKFeature = static_cast<TopKFeature const *>(feature);
      return topKFeature->getFrequencies()[index1];    
    };
    auto funcToken1 = std::make_shared<FuncToken<Netflow>>(featureMap, 
                                                      function1, identifier);

    // Addition token
    auto addOper = std::make_shared<AddOperator<Netflow>>(featureMap);

    // Second function token
    int index2 = 0;
    auto function2 = [&index2](Feature const * feature)->double {
      auto topKFeature = static_cast<TopKFeature const *>(feature);
      return topKFeature->getFrequencies()[index2];    
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
     
    int queueLength = 1000; 
    auto filter = std::make_shared<Filter<Netflow, DestIp>>(
      *filterExpression, nodeId, featureMap, "servers", queueLength);
    consumer->registerConsumer(filter);

}

int main(int argc, char** argv) {

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

  // The total number of elements in a sliding window
  int N;

  // The number of elements in a dormant or active window
  int b;

  int nop; //not used

  // The file location of labeled instances. 
  string learnfile;

  time_t timestamp_sec1, timestamp_sec2;

  // The training data if learning the classifier
  arma::mat trainingData;

  string inputfile = "";

  string outputfile = "";

  // The model that can be trained from example data or if a trained model
  // exists, can be loaded from the filesystem.
  NBCModel model;

  // The capacity of the FeatureMap and FeatureSubscriber data structures.
  int capacity = 10000;

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
    ("nop", po::value<int>(&nop)->default_value(1),
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
    ("capacity", po::value<int>(&capacity)->default_value(10000),
      "The capacity of the FeatureMap and FeatureSubcriber")
  ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 1;
  }

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

  // Creating the ZeroMQPushPull consumer.  This consumer is responsible for
  // getting the data from the receiver (e.g. a socket or a file) and then
  // publishing it in a load-balanced way to the cluster.
  int queueLength = 1000;
  auto consumer = std::make_shared<ZeroMQPushPull>(queueLength,
                                 numNodes, 
                                 nodeId, 
                                 hostnames, 
                                 ports, 
                                 hwm);

  // The global featureMap (global for all features generated for this node,
  // each node has it's own featuremap.
  FeatureMap featureMap;

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

    ReadCSV receiver(inputfile);
    receiver.registerConsumer(consumer);

    int numFeatures = 1;
    auto subscriber = std::make_shared<FeatureSubscriber>(capacity);
                                                          
    createPipeline(consumer, featureMap, subscriber, nodeId);

    subscriber->init();

    if (!receiver.connect()) {
      std::cout << "Problems opening file " << learnfile << std::endl;
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

    // Get the features that had been created and write them out to outputfile
    std::string result = subscriber->getOutput();
    //result is empty and there are two catches that probably should be removed.
    //pullthreads not ending I think which makes it hang
    std::cout << "result " << result << std::endl;
    std::ofstream out(outputfile);
    out << result;
    out.close();
    return 0;
 
  }  
  /********************* Learning Model *********************************/
  else if (vm.count("train"))
  {
    // The true parameter transposes the data.  In mlpack, rows are features 
    // and columns are observations, which makes things confusing.
    //data::Load(learnfile, trainingData, true);

    //arma::Row<size_t> labels;

    //data::NormalizeLabels(trainingData.row(trainingData.n_rows - 1), labels,
    //                      model.mappings);
    // Remove the label row
    //trainingData.shed_row(trainingData.n_rows - 1);

    ReadCSV receiver(learnfile);
    receiver.registerConsumer(consumer);    

    std::vector<std::string> featureNames;
    //createPipeline(consumer, featureMap, featureNames, nodeId);

    if (!receiver.connect()) {
      std::cout << "Problems opening file " << learnfile << std::endl;
      return -1;
    }

   

    //Timer::Start("nbc_training");
    //model.nbc = NaiveBayesClassifier<>(trainingData, labels,
    //  model.mappings.n_elem, true);
    //Timer::Stop("nbc_training");

    //if (modelfile != "") {
    //  data::Save(modelfile, "model", model);
    //}

  } 
  /******************** Applying model *********************************/
  else if (vm.count("test"))
  {

  }
  /******************* Running pipeline without model ******************/
  else 
  {

    ReadSocket receiver(ip, ncPort);
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

