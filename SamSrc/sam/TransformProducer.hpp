#ifndef TRANSFORM_PRODUCER_HPP
#define TRANSFORM_PRODUCER_HPP

#include <queue>
#include <sam/TupleExpression.hpp>
#include <sam/FeatureMap.hpp>
#include <sam/Tokens.hpp>
#include <sam/AbstractConsumer.hpp>
#include <sam/BaseComputation.hpp>
#include <sam/BaseProducer.hpp>
#include <sam/Util.hpp>
#include <sam/tuples/Edge.hpp>

namespace sam {


template <typename InputEdgeType, 
          typename OutputEdgeType, size_t... keyFields>
class TransformProducer : public AbstractConsumer<InputEdgeType>,
                          public BaseComputation,
                          public BaseProducer<OutputEdgeType>
{
public:
  typedef typename InputEdgeType::LocalTupleType InputTupleType;
  //typedef typename InputEdgeType::LocalIdType    IdType;
  //typedef typename InputEdgeType::LocalLabelType LabelType;
  //typedef typename InputEdgeType::LocalTupleType TupleType;
  
private:
  std::shared_ptr<const TupleExpression<InputTupleType>> transformExpressions;

public:
  TransformProducer(std::shared_ptr<const TupleExpression<InputTupleType>> 
                      _expression,
                      size_t nodeId,
                      std::shared_ptr<FeatureMap> featureMap,
                      std::string identifier,
                      size_t queueLength);

  virtual ~TransformProducer();

  virtual bool consume(InputEdgeType const& edge);

  void terminate();

};

template <typename InputEdgeType, 
          typename OutputEdgeType, size_t... keyFields>
void TransformProducer<InputEdgeType, 
                       OutputEdgeType, keyFields...>::terminate()
{
  for (auto consumer : this->consumers) {
    consumer->terminate();
  }
}

template <typename InputEdgeType, 
          typename OutputEdgeType, size_t... keyFields>
TransformProducer<InputEdgeType, OutputEdgeType, keyFields...>::
TransformProducer(
  std::shared_ptr<const TupleExpression<InputTupleType>> expression,
  size_t nodeId,
  std::shared_ptr<FeatureMap> featureMap,
  std::string identifier,
  size_t queueLength)
  :
  BaseComputation(nodeId, featureMap, identifier),
  BaseProducer<OutputEdgeType>(nodeId, queueLength),
  transformExpressions(expression) 
{
}

template <typename InputEdgeType, 
          typename OutputEdgeType, size_t... keyFields>
TransformProducer<InputEdgeType, 
                  OutputEdgeType, keyFields...>::~TransformProducer()
{
}

template <typename InputEdgeType,
          typename OutputEdgeType, size_t... keyFields>
bool 
TransformProducer<InputEdgeType, OutputEdgeType, keyFields...>::
consume(InputEdgeType const& edge)
{
  std::string key = generateKey<keyFields...>(edge.tuple);

  // Generate a subtuple with just the key fields
  std::index_sequence<keyFields...> sequence;
  auto outTuple = subtuple(edge.tuple, sequence);

  //TODO: This only works if there is only one transform expression
  double result = 0;
  
  bool b = transformExpressions->get(0)->evaluate(key, 
                                                  edge.tuple, result);
  std::tuple<double> resultTuple = std::make_tuple(result);
  auto finalTuple = std::tuple_cat(outTuple, resultTuple);

  OutputEdgeType outputEdgeType(edge.id, edge.label, finalTuple);

  this->parallelFeed(outputEdgeType);

  return true;
}

}
#endif
