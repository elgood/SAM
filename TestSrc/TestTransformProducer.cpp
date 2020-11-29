#define BOOST_TEST_MAIN TestTransformProducer
#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <vector>
#include <sam/TupleExpression.hpp>
#include <sam/FeatureMap.hpp>
#include <sam/FeatureSubscriber.hpp>
#include <sam/TransformProducer.hpp>
#include <sam/tuples/VastNetflow.hpp>
#include <sam/tuples/Edge.hpp>
#include <sam/tuples/Tuplizer.hpp>
#include <sam/Identity.hpp>

using namespace sam;
using namespace sam::vast_netflow;

typedef std::tuple<std::string, std::string, double> TimeLapseDestSrc;
typedef VastNetflow TupleType;
typedef EmptyLabel LabelType;
typedef Edge<size_t, LabelType, TupleType> InputEdgeType;
typedef Edge<size_t, LabelType, TimeLapseDestSrc> OutputEdgeType;
typedef TuplizerFunction<InputEdgeType, MakeVastNetflow> Tuplizer;

BOOST_AUTO_TEST_CASE( test_transform_producer )
{
  #define DestIp_TimeLapseSeries    0 
  #define SrcIp_TimeLapseSeries     1
  #define TimeDiff_TimeLapseSeries  2 

  auto featureMap = std::make_shared<FeatureMap>();

  std::vector<std::shared_ptr<Expression<VastNetflow>>> expressions;
  std::vector<std::string> names;
  names.push_back("TimeDiff");

  // Expression TimeSeconds - Prev.TimeSeconds
  // 
  // TimeSeconds field token 
  std::shared_ptr<ExpressionToken<VastNetflow>> fieldToken = 
    std::make_shared<FieldToken<TimeSeconds, VastNetflow>>(featureMap);
  // Sub operator token
  std::shared_ptr<ExpressionToken<VastNetflow>> subToken = std::make_shared<
    SubOperator<VastNetflow>>(featureMap);
  // Prev.TimeSeconds
  std::shared_ptr<ExpressionToken<VastNetflow>> prevToken = std::make_shared<
    PrevToken<TimeSeconds, VastNetflow>>(featureMap);

  std::list<std::shared_ptr<ExpressionToken<VastNetflow>>> infixList;
  infixList.push_back(fieldToken);
  infixList.push_back(subToken);
  infixList.push_back(prevToken);

  auto expression = std::make_shared<Expression<VastNetflow>>( infixList );
  expressions.push_back(expression);
  
  auto tupleExpression =
    std::make_shared<TupleExpression<VastNetflow>>(expressions);
  size_t nodeId = 0;
  std::string identifier = "destsrc_timelapseseries";

  // queueLength determines how many inputs we see before feeding it out
  // in parallel via BaseProducer::paralleFeed.  We don't want to have to 
  // fill up the queue but want immediate response.
  int queueLength = 1;

  //typedef TuplizerFunction<EdgeType, MakeVastNetflow>
  //  Tuplizer;
  auto timeLapseSeries = 
    new TransformProducer<InputEdgeType,
                          OutputEdgeType, 
                               DestIp, SourceIp>
                              (tupleExpression,
                               nodeId,
                               featureMap,
                               identifier,
                               queueLength);

  // We'll use a feature subscriber and an identity operator to accumulate
  // the timediff values from the TransformProducer.
  int numFeatures = 1;
  std::string outputFile = "outputFileTestTransformProducer.txt";
  auto subscriber = std::make_shared<FeatureSubscriber>(outputFile, numFeatures);


  identifier = "identity";
  auto identity = std::make_shared<Identity<OutputEdgeType, 
                    TimeDiff_TimeLapseSeries, DestIp_TimeLapseSeries>>(0, 
                    featureMap, "identity");
  
  // The identity operator gets its input from the timeLapseSeries
  timeLapseSeries->registerConsumer(identity);

  // The subscriber listens to the features produced by the identity operator
  identity->registerSubscriber(subscriber, identifier);

  subscriber->init();

  // We create a bunch or netflows where the only difference is the time, 
  // which we increase by 1 second for each netflow.
  std::string after = ",2013-04-10 08:32:36,"
                     "20130410083236.384094,17,UDP,172.20.2.18,"
                     "239.255.255.250,29986,1900,0,0,0,133,0,1,0,1,0,0";

  Tuplizer tuplizer;
  int numExamples = 100;
  for (int i = 0; i < numExamples; i++) {

    // Creating the string that represents the netflow and feeding it to
    // the timeLapseSeries.
    std::string s = boost::lexical_cast<std::string>(i) + after;
    InputEdgeType edge = tuplizer(i, s);
    timeLapseSeries->consume(edge);
  }

  // Each line should contain the number 1
  auto infile = std::ifstream(outputFile);
  std::string line;
  int i = 0;
  while(std::getline(infile, line)) {
    // The first line is 0 since there wasn't a previous value to work with.
    if (i == 0) {
      BOOST_CHECK_EQUAL(boost::lexical_cast<double>(line), 0.0);
    }
    if (i > 0) {
      BOOST_CHECK_EQUAL(boost::lexical_cast<double>(line), 1.0);
    }
    i++;

  }
  BOOST_CHECK_EQUAL(i, numExamples);
  //std::string result = subscriber->getOutput();  
  //boost::char_separator<char> newline("\n");
  //boost::tokenizer<boost::char_separator<char>> newlineTok(result, newline);
  //int i = 0;
  //BOOST_FOREACH(std::string const& line, newlineTok)
  //{
  //}



}
