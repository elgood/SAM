#define BOOST_TEST_MAIN TestTransformProducer
#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <vector>
#include "TupleExpression.hpp"
#include "FeatureMap.hpp"
#include "TransformProducer.hpp"
#include "Netflow.hpp"

using namespace sam;

BOOST_AUTO_TEST_CASE( test_transform_producer )
{

  #define DestIp_TimeLapseSeries    0
  #define SrcIp_TimeLapseSeries     1
  #define TimeDiff_TimeLapseSeries  2 
  typedef std::tuple<std::string, std::string, double> TimeLapseDestSrc;

  FeatureMap destSrcFeatureMap;

  std::vector<Expression<Netflow>> expressions;
  std::vector<std::string> names;
  names.push_back("TimeDiff");

  // Expression TimeSeconds - Prev.TimeSeconds
  // 
  // TimeSeconds field token 
  std::shared_ptr<ExpressionToken<Netflow>> fieldToken = std::make_shared<
    FieldToken<TIME_SECONDS_FIELD, Netflow>>(destSrcFeatureMap);
  // Sub operator token
  std::shared_ptr<ExpressionToken<Netflow>> subToken = std::make_shared<
    SubOperator<Netflow>>(destSrcFeatureMap);
  // Prev.TimeSeconds
  std::shared_ptr<ExpressionToken<Netflow>> prevToken = std::make_shared<
    PrevToken<TIME_SECONDS_FIELD, Netflow>>(destSrcFeatureMap);

  std::list<std::shared_ptr<ExpressionToken<Netflow>>> infixList;
  infixList.push_back(fieldToken);
  infixList.push_back(subToken);
  infixList.push_back(prevToken);

  Expression<Netflow> expression(infixList);
  expressions.push_back(expression);
  
  TupleExpression<Netflow> tupleExpression(expressions, names);
  size_t nodeId = 0;
  std::string identifier = "destsrc_timelapseseries";
  int queueLength = 1000;

  auto timeLapseSerieis = new TransformProducer<Netflow, TimeLapseDestSrc, 
                               DEST_IP_FIELD, SOURCE_IP_FIELD>
                              (tupleExpression,
                               nodeId,
                               destSrcFeatureMap,
                               identifier,
                               queueLength);


}
