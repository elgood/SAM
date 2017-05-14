#ifndef TRANSFORM_PRODUCER_HPP
#define TRANSFORM_PRODUCER_HPP

#include <queue>
#include "TupleExpression.hpp"
#include "FeatureMap.hpp"
#include "Tokens.hpp"
#include "AbstractConsumer.hpp"
#include "BaseComputation.hpp"
#include "BaseProducer.hpp"
#include "Util.hpp"

namespace sam {


template <typename InputType, typename OutputType, size_t... keyFields>
class TransformProducer : public AbstractConsumer<InputType>,
                          public BaseComputation<0, keyFields...>,
                          public BaseProducer<OutputType>
{
private:
  TupleExpression<InputType> const& transformExpressions;

public:
  TransformProducer(TupleExpression<InputType> const& _expression,
                      size_t nodeId,
                      FeatureMap& featureMap,
                      string identifier,
                      size_t queueLength);
                      //size_t historyLength);
  virtual ~TransformProducer();

  virtual bool consume(InputType const& input);

};

template <typename InputType, typename OutputType, size_t... keyFields>
TransformProducer<InputType, OutputType, keyFields...>::TransformProducer(
  TupleExpression<InputType> const& expression,
  size_t nodeId,
  FeatureMap& featureMap,
  string identifier,
  size_t queueLength)
  :
  BaseComputation<0, keyFields...>(nodeId, featureMap, identifier),
  BaseProducer<OutputType>(queueLength),
  transformExpressions(expression) 
{
}

template <typename InputType, typename OutputType, size_t... keyFields>
TransformProducer<InputType, OutputType, keyFields...>::~TransformProducer()
{
}

template <typename InputType, typename OutputType, size_t... keyFields>
bool TransformProducer<InputType, OutputType, keyFields...>::consume(
  InputType const& input)
{
  string key = this->generateKey(input);

  // Generate a subtuple with just the key fields
  std::index_sequence<keyFields...> sequence;
  auto outTuple = subtuple(input, sequence);

 

  // Add the new fields from the transform expressions. 
  //TupleExpression::const_iterator iterator = transformExpressions.begin(); 
  for ( auto expression : transformExpressions ) {

    double result = 0;
    bool b = expression.evaluate(key, input, result);
  }

  return true;
}

/*
void TransformProducer<InputType, OutputType>::updateHistory(
  InputType const& input)
{
  std::string key = generateKey(t);

  try {
    OutputType tuple;

    //std::get<

    for ( auto expression : TupleExpression )
    {
      double result = expression.evaluate(key, this->featureMap);

    }

  }

  for (int i = 0; i < historyLength - 1; i++) {
    std::string featureName = generateHistoryIdentifier(i+1);
    auto feature = featureMap.at( key, featureName );
    featureName = generateHistoryIdentifier(i+2);
    featureMap.updateInsert( key, featureName, *feature );
  }
 
  TupleFeature tupleFeature( input ); 
  featureMap.updateInsert( key, generateKey(1), input );
}
*/


}
#endif
