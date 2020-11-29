#ifndef FILTER_HPP
#define FILTER_HPP

#include <string>
#include <sam/Expression.hpp>
#include <sam/AbstractConsumer.hpp>
#include <sam/BaseComputation.hpp>
#include <sam/BaseProducer.hpp>

using std::string;

namespace sam {

template <typename EdgeType, size_t... keyFields>
class Filter: public AbstractConsumer<EdgeType>, 
              public BaseComputation,
              public BaseProducer<EdgeType>
{
public:
  typedef typename EdgeType::LocalTupleType TupleType;
private:
  std::shared_ptr<const Expression<TupleType>> expression;
public:
  Filter(std::shared_ptr<const Expression<TupleType>> _exp,
         size_t nodeId,
         std::shared_ptr<FeatureMap> featureMap,
         string identifier,
         size_t queueLength) :
         BaseComputation(nodeId, featureMap, identifier), 
         BaseProducer<EdgeType>(nodeId, queueLength),
         expression(_exp)
  {}

  bool consume(EdgeType const& edge);

  void terminate();

};

template <typename EdgeType, size_t... keyFields>
bool Filter<EdgeType, keyFields...>::consume(EdgeType const& edge) 
                                              
{
  string key = generateKey<keyFields...>(edge.tuple);
  double result = 0;
  bool b = expression->evaluate(key, edge.tuple, result); 
  if (b) {
    BooleanFeature feature(result);
    this->featureMap->updateInsert(key, this->identifier, feature); 
    if ( result ) {
      this->parallelFeed(edge);
    } else {
      BooleanFeature feature(0);
      this->featureMap->updateInsert(key, this->identifier, feature);  
    }
  }

  this->parallelFeed(edge);

  return true;
}

template <typename EdgeType, size_t... keyFields>
void Filter<EdgeType, keyFields...>::terminate()
{
  for (auto consumer : this->consumers) {
    consumer->terminate();
  }
}


}

#endif
