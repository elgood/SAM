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
  std::shared_ptr<const TupleExpression<InputType>> transformExpressions;

public:
  TransformProducer(std::shared_ptr<const TupleExpression<InputType>> 
                      _expression,
                      size_t nodeId,
                      std::shared_ptr<FeatureMap> featureMap,
                      std::string identifier,
                      size_t queueLength);
                      //size_t historyLength);
  virtual ~TransformProducer();

  virtual bool consume(InputType const& input);

  void terminate();

};

template <typename InputType, typename OutputType, size_t... keyFields>
void TransformProducer<InputType, OutputType, keyFields...>::terminate()
{
  for (auto consumer : this->consumers) {
    consumer->terminate();
  }
}

template <typename InputType, typename OutputType, size_t... keyFields>
TransformProducer<InputType, OutputType, keyFields...>::TransformProducer(
  std::shared_ptr<const TupleExpression<InputType>> expression,
  size_t nodeId,
  std::shared_ptr<FeatureMap> featureMap,
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
  //std::cout << "TransformProducer " << toString(input) << std::endl;
  std::string key = generateKey<keyFields...>(input);
  //std::cout << "key " << key << std::endl;

  // Generate a subtuple with just the key fields
  // 0 is included to get the SamGenerated id
  std::index_sequence<0, keyFields...> sequence;
  auto outTuple = subtuple(input, sequence);
  //std::cout << "outTuple " << toString(outTuple) << std::endl;

  //TODO: This only works if there is only one transform expression
  double result = 0;
  
  //auto expression = transformExpressions.get(0);
  bool b = transformExpressions->get(0)->evaluate(key, input, result);
  //std::cout << "After transformExpression " << std::endl;
  auto finalTuple = std::tuple_cat(outTuple, std::tie(result));

  //std::cout << "Before parallel feed " << std::endl;
  this->parallelFeed(finalTuple);

  return true;
}

}
#endif
