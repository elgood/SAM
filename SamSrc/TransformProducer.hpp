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
                          public BaseComputation,
                          public BaseProducer<OutputType>
{
private:
  TupleExpression<InputType> const& transformExpressions;

public:
  TransformProducer(TupleExpression<InputType> const& _expression,
                      size_t nodeId,
                      FeatureMap& featureMap,
                      std::string identifier,
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
  std::string identifier,
  size_t queueLength)
  :
  BaseComputation(nodeId, featureMap, identifier),
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
  std::string key = generateKey<keyFields...>(input);

  // Generate a subtuple with just the key fields
  std::index_sequence<keyFields...> sequence;
  auto outTuple = subtuple(input, sequence);
 

  // Add the new fields from the transform expressions. 
  for ( auto expression : transformExpressions ) {

    double result = 0;
    bool b = expression.evaluate(key, input, result);
  }

  return true;
}

}
#endif
