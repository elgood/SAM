/**
 * Performs some tests using the CTU data
 */

#define BOOST_TEST_MAIN TestCTU
#include <boost/test/unit_test.hpp>
#include <boost/lexical_cast.hpp>
#include <fstream>
#include <iostream>
#include <stdio.h>
#include <deque>

#include <sam/tuples/VastNetflow.hpp>
#include <sam/tuples/Edge.hpp>
#include <sam/tuples/Tuplizer.hpp>
#include <sam/FeatureSubscriber.hpp>
#include <sam/ReadCSV.hpp>
#include <sam/LabelProducer.hpp>
#include <sam/ExponentialHistogramSum.hpp>
#include <sam/ExponentialHistogramVariance.hpp>
#include <sam/ZeroMQPushPull.hpp>

using namespace sam;
using namespace sam::vast_netflow;

typedef VastNetflow TupleType;
typedef SingleBoolLabel LabelType;
typedef Edge<size_t, LabelType, TupleType> EdgeType;
typedef TuplizerFunction<EdgeType, MakeVastNetflow> Tuplizer;

BOOST_AUTO_TEST_CASE( test_sample )
{
  // valid if called from SAM/build with tests/TestCTU.  TODO:
  // make more robust to different locations of build directory 
  //std::string dataFile = "../TestSrc/Data/CTU1SampleAsVast.csv";
  std::string dataFile = "../../TestSrc/Data/147.32.84.229.csv";
 
  Tuplizer tuplizer;
  
  // Calculating some stats
  std::ifstream infile;
  infile.open( dataFile );
  if (infile.is_open())
  {
    int numPos = 0;
    int numNeg = 0;
    int total = 0;
    
    std::string line;
    while (std::getline(infile, line))
    {
      auto edge = tuplizer(total, line);
      int label = boost::lexical_cast<int>(std::get<0>(edge.label)); 
      if (label == 0) {
        numNeg++;
      } else {
        numPos++;
      }
      total++;
    }

    std::cout << "Num Negative " <<  numNeg 
              << " Num Positive " << numPos << std::endl;

    infile.close();

    std::size_t nodeId = 0; ///> The node id of this node
    int capacity = 100000;
    auto featureMap = std::make_shared<FeatureMap>(capacity);
    std::string outputfile = "TestCTUOutputFile.txt";
    auto subscriber = 
      std::make_shared<FeatureSubscriber>(outputfile, capacity);
    auto receiver = 
      std::make_shared<ReadCSV<EdgeType, Tuplizer>>(nodeId, dataFile);
    std::size_t numNodes = 1; ///> The number of nodes in the cluster
    
    // Get the label
    // Doesn't really need a key, but provide one anyway to the template.
    //TODO Label
    std::string identifier = "label";

    auto label = std::make_shared<LabelProducer<EdgeType, DestIp>>
                (nodeId, featureMap, identifier);
    receiver->registerConsumer(label);
    label->registerSubscriber(subscriber, identifier);

    identifier = "averageSrcTotalBytes";
    int N = 189;
    auto averageSrcTotalBytes = std::make_shared<
                      ExponentialHistogramAve<double, EdgeType,
                                                 SrcTotalBytes,
                                                 DestIp>>
                          (N, 2, nodeId, featureMap, identifier);
    receiver->registerConsumer(averageSrcTotalBytes); 
    averageSrcTotalBytes->registerSubscriber(subscriber, identifier);

    identifier = "varSrcTotalBytes";
    auto varSrcTotalBytes = 
      std::make_shared<ExponentialHistogramVariance<
        double, EdgeType, SrcTotalBytes, DestIp>>
                               (N, 2, nodeId, featureMap, identifier);
    receiver->registerConsumer(varSrcTotalBytes);
    varSrcTotalBytes->registerSubscriber(subscriber, identifier);

    subscriber->init();

    if (!receiver->connect()) {
      throw std::runtime_error("Problems opening data file");
    }

    receiver->receive();
    //delete consumer;

    int numPosFound = 0;
    int numNegFound = 0;

    // Open the original file
    std::ifstream origfile;
    origfile.open( dataFile );

    // Open the result file
    std::ifstream resultfile = std::ifstream(outputfile);

    // Going to read through the original data file and the result string
    // simultaneously.  There should be a one-to-one correspondence.
    if (resultfile.is_open() && origfile.is_open())
    {
      //Sum of SrcTotalBytes per dest ip
      std::map<std::string, std::deque<long>> valuesSrcTotalBytes; 
      int numLines = 0;
      double totalDiffMeanSrcTotalBytes = 0;
      double totalDiffVarSrcTotalBytes = 0;

      bool anotherLine = false;
      // Get a line from the original data file
      std::string orig;
      std::string result;
      while (std::getline(origfile, orig) && std::getline(resultfile, result))
      {
      
        // Tokenize the line from the original data file
        //std::vector<std::string> lineVector = convertToTokens(orig);
        auto edge = tuplizer(numLines, orig);
        auto tuple = edge.tuple;
        // Get the destIp, which is the key for imux operation
        std::string destIp = std::get<DestIp>(tuple);
        std::cout << "destIP " << destIp << std::endl;

        // Calculate the exact value of the features from the original 
        // data file. 

        // Exact average SrcTotalBytes over last N netflows with same DestIp
        long srcTotalBytes = boost::lexical_cast<long>(std::get<SrcTotalBytes>(tuple));
        std::cout << "srcTotalBytes" << srcTotalBytes << std::endl;      
        if (valuesSrcTotalBytes.count(destIp) == 0) {//Create deque if not there
          valuesSrcTotalBytes[destIp] = std::deque<long>( );
        } 
        if (valuesSrcTotalBytes[destIp].size() >= N) { //Pop item if too many
          valuesSrcTotalBytes[destIp].pop_back();
        }
        valuesSrcTotalBytes[destIp].push_front(srcTotalBytes); //Push new value
        double expMeanSrcTotalBytes = calcMean( valuesSrcTotalBytes[destIp] );

        // Exact variance SrcTotalBytes over last N netflows with same DestIp
        double expVarSrcTotalBytes = calcStandardDeviation( 
                                      valuesSrcTotalBytes[destIp] );
        expVarSrcTotalBytes *= expVarSrcTotalBytes;

        // Convert the current line from the result string to tokens
        std::vector<std::string> tokenVector = convertToTokens(result);

        // Updating counts of num negative examples and num positive examples
        if (tokenVector[0] == "0") numNegFound++;
        if (tokenVector[0] == "1") numPosFound++;

        std::cout << "result " << result << std::endl;
        double predMeanSrcTotalBytes = boost::lexical_cast<double>(
                                          tokenVector[1]);
        double predVarSrcTotalBytes = boost::lexical_cast<double>(
                                          tokenVector[2]);
        std::cout <<"destIp " << destIp << " srcTotalBytes " << srcTotalBytes 
                  << " expMeanSrcTotalBytes " << expMeanSrcTotalBytes 
                  << " predMeanSrcTotalBytes " << predMeanSrcTotalBytes 
                  << " expVarSrcTotalBytes " << expVarSrcTotalBytes 
                  << " predVarSrcTotalBytes " << predVarSrcTotalBytes 
                  << " size "<< valuesSrcTotalBytes[destIp].size() << std::endl;
        
        //std::cout << "totalDiffVarSrcTotalBytes " << totalDiffVarSrcTotalBytes
        //          << std::endl;
        if (expMeanSrcTotalBytes < predMeanSrcTotalBytes) {
          totalDiffMeanSrcTotalBytes += 
            predMeanSrcTotalBytes / expMeanSrcTotalBytes;
        } else {
          totalDiffMeanSrcTotalBytes += 
            expMeanSrcTotalBytes / predMeanSrcTotalBytes;
        }
        if (expVarSrcTotalBytes > 0 && predVarSrcTotalBytes > 0) {
          if (expVarSrcTotalBytes < predVarSrcTotalBytes) {
            totalDiffVarSrcTotalBytes += 
              std::sqrt(predVarSrcTotalBytes) / std::sqrt(expVarSrcTotalBytes);
          } else {
            totalDiffVarSrcTotalBytes += 
              std::sqrt(expVarSrcTotalBytes) / std::sqrt(predVarSrcTotalBytes);
          }
        }
        std::cout << "totalDiffVarSrcTotalBytes " << totalDiffVarSrcTotalBytes
                  << std::endl;

        numLines++;
        // Comparing predicated and expected SrcTotalBytes
        // Individual differences can be big, especially when there is a 
        // deletion of a large item.  Later we gather overall differences,
        // which is much smaller
        BOOST_CHECK_CLOSE(predMeanSrcTotalBytes, expMeanSrcTotalBytes, 300);
        BOOST_CHECK_CLOSE(std::sqrt(predVarSrcTotalBytes), 
                          std::sqrt(expVarSrcTotalBytes), 2100);

                 
        // SrcTotalBytes can't be negative
        BOOST_CHECK(predMeanSrcTotalBytes >= 0);
        BOOST_CHECK(predVarSrcTotalBytes >= 0);
      }
      std::cout << "average diff " << totalDiffMeanSrcTotalBytes / numLines
                << std::endl;
      std::cout << "average diff " << totalDiffVarSrcTotalBytes / numLines
                << std::endl;
      BOOST_CHECK_CLOSE(1, totalDiffMeanSrcTotalBytes/ numLines, 50);
      BOOST_CHECK_CLOSE(1, totalDiffVarSrcTotalBytes/ numLines, 50); 

      BOOST_CHECK_EQUAL(numNeg, numNegFound);
      BOOST_CHECK_EQUAL(numPos, numPosFound);

    } else {
      throw std::runtime_error("Problems opening data file");
    }
    remove(outputfile.c_str());
  } else {
    std::cout << "Couldn't read file " << std::endl;
    throw std::runtime_error("Problem with test.  Couldn't read data file");
  }
  std::cout << "The end " << std::endl;
}
