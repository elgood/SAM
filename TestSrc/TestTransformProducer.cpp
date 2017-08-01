#define BOOST_TEST_MAIN TestTransformProducer
#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <vector>
#include "TupleExpression.hpp"
#include "FeatureMap.hpp"
#include "FeatureSubscriber.hpp"
#include "TransformProducer.hpp"
#include "Netflow.hpp"
#include "Identity.hpp"

using namespace sam;

BOOST_AUTO_TEST_CASE( test_transform_producer )
{
  #define DestIp_TimeLapseSeries    1 
  #define SrcIp_TimeLapseSeries     2
  #define TimeDiff_TimeLapseSeries  3 
  typedef std::tuple<std::size_t, std::string, std::string, double> 
    TimeLapseDestSrc;

  auto featureMap = std::make_shared<FeatureMap>();

  std::vector<Expression<Netflow>> expressions;
  std::vector<std::string> names;
  names.push_back("TimeDiff");

  // Expression TimeSeconds - Prev.TimeSeconds
  // 
  // TimeSeconds field token 
  std::shared_ptr<ExpressionToken<Netflow>> fieldToken = std::make_shared<
    FieldToken<TimeSeconds, Netflow>>(featureMap);
  // Sub operator token
  std::shared_ptr<ExpressionToken<Netflow>> subToken = std::make_shared<
    SubOperator<Netflow>>(featureMap);
  // Prev.TimeSeconds
  std::shared_ptr<ExpressionToken<Netflow>> prevToken = std::make_shared<
    PrevToken<TimeSeconds, Netflow>>(featureMap);

  std::list<std::shared_ptr<ExpressionToken<Netflow>>> infixList;
  infixList.push_back(fieldToken);
  infixList.push_back(subToken);
  infixList.push_back(prevToken);

  Expression<Netflow> expression(infixList);
  expressions.push_back(expression);
  
  TupleExpression<Netflow> tupleExpression(expressions);
  size_t nodeId = 0;
  std::string identifier = "destsrc_timelapseseries";

  // queueLength determines how many inputs we see before feeding it out
  // in parallel via BaseProducer::paralleFeed.  We don't want to have to 
  // fill up the queue but want immediate response.
  int queueLength = 1;

  auto timeLapseSeries = new TransformProducer<Netflow, TimeLapseDestSrc, 
                               DestIp, SourceIp>
                              (tupleExpression,
                               nodeId,
                               featureMap,
                               identifier,
                               queueLength);

  // We'll use a feature subscriber and an identity operator to accumulate
  // the timediff values from the TransformProducer.
  int numFeatures = 1;
  auto subscriber = std::make_shared<FeatureSubscriber>(numFeatures);

  identifier = "identity";
  auto identity = std::make_shared<Identity<TimeLapseDestSrc, 
                    TimeDiff_TimeLapseSeries, DestIp_TimeLapseSeries>>(0, 
                    featureMap, "identity");
  
  // The identity operator gets its input from the timeLapseSeries
  timeLapseSeries->registerConsumer(identity);

  // The subscriber listens to the features produced by the identity operator
  identity->registerSubscriber(subscriber, identifier);

  subscriber->init();

  // We create a bunch or netflows where the only difference is the time, which
  // we increase by 1 second for each netflow.
  std::string before = "1,1,";
  std::string after = ",2013-04-10 08:32:36,"
                     "20130410083236.384094,17,UDP,172.20.2.18,"
                     "239.255.255.250,29986,1900,0,0,0,133,0,1,0,1,0,0";

  int numExamples = 100;
  for (int i = 0; i < numExamples; i++) {

    // Creating the string that represents the netflow and feeding it to
    // the timeLapseSeries.
    std::string s = before + boost::lexical_cast<std::string>(i) + after;
    Netflow netflow = makeNetflow(s);
    timeLapseSeries->consume(netflow);
  }

  // Each line should contain the number 1
  std::string result = subscriber->getOutput();  
  boost::char_separator<char> newline("\n");
  boost::tokenizer<boost::char_separator<char>> newlineTok(result, newline);
  int i = 0;
  BOOST_FOREACH(std::string const& line, newlineTok)
  {
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



}
