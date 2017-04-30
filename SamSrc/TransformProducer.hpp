#ifndef TRANSFORM_PRODUCER_HPP
#define TRANSFORM_PRODUCER_HPP

#include <queue>
#include "TupleExpression.hpp"
#include "FeatureMap.hpp"
#include "Tokens.hpp"

namespace sam {


template <typename InputType, typename OutputType>
class TransformProducer : public AbstractConsumer<InputType>,
                          public BaseComputation,
                          public BaseProducer<OutputType>
{
private:
  TupleExpression const& expression;

  // How many previous values we need to keep
  size_t historyLength;

  // An fixed-size array that holds a history of the inputs.
  InputType *inputHistory;

  // The index to the most recent entry in the inputHistory
  size_t index;
public:
  TransformProducer(TupleExpression const& _expression,
                      vector<size_t> keyFields,
                      size_t nodeId,
                      FeatureMap& featureMap,
                      string identifier, 
                      size_t queueLength,
                      size_t historyLength);
  virtual ~TransformProducer();

  virtual bool consume(InputType const& input);

private:
  void updateHistory(InputType const& input);
 
  std::string generateHistoryIdentifier(size_t i);
};

template <typename InputType, typename OutputType>
TransformProducer<InputType, OutputType>::TransformProducer(
  TupleExpression const& _expression,
  vector<size_t> keyFields,
  size_t nodeId,
  FeatureMap& featureMap,
  string identifier, 
  size_t queueLength,
  size_t historyLength) 
  :
  TupleExpression(_expression), 
  BaseComputation(keyFields, 0, nodeId, featureMap, identifier),
  BaseProducer(queueLength)
{
  this->historyLegnth = historyLength;
  inputHistory = new InputType[historyLength];
  index = 0;
}

template <typename InputType, typename OutputType>
TransformProducer<InputType, OutputType>::~TransformProducer()
{
  delete[] inputHistory;
}

template <typename InputType, typename OutputType>
bool TransformProducer<InputType, OutputType>::consume(InputType const& input)
{
  string key = generateKey(t);

  OutputType result = expression.evaluate(key, featureMap); 
   
  updateHistory(input); 
}

template <typename InputType, typename OutputType>
void TransformProducer<InputType, OutputType>::updateHistory(
  InputType const& input)
{
  std::string key = generateKey(t);

  for (int i = 0; i < historyLength - 1; i++) {
    std::string featureName = generateHistoryIdentifier(i+1);
    auto feature = featureMap.at( key, featureName );
    featureName = generateHistoryIdentifier(i+2);
    featureMap.updateInsert( key, featureName, *feature );
  }
 
  TupleFeature tupleFeature( input ); 
  featureMap.updateInsert( key, generateKey(1), input );
}



}
#endif
