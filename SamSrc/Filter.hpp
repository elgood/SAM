#ifndef FILTER_HPP
#define FILTER_HPP

#include <string>
#include "Expression.hpp"

using std::string;

namespace sam {

template <typename TupleType, size_t... keyFields>
class Filter: public AbstractConsumer<TupleType>, 
              public BaseComputation,
              public BaseProducer<TupleType>
{
private:
  std::shared_ptr<const Expression<TupleType>> expression;
public:
  Filter(std::shared_ptr<const Expression<TupleType>> _exp,
         size_t nodeId,
         std::shared_ptr<FeatureMap> featureMap,
         string identifier,
         size_t queueLength) :
         BaseComputation(nodeId, featureMap, identifier), 
         BaseProducer<TupleType>(queueLength),
         expression(_exp)
  {}

  bool consume(TupleType const& t);

};

template <typename TupleType, size_t... keyFields>
bool Filter<TupleType, keyFields...>::consume(TupleType const& t) 
{
  string key = generateKey<keyFields...>(t);
  //std::cout << "key " << key << std::endl;
  double result = 0;
  bool b = expression->evaluate(key, t, result); 
  //std::cout << "result " << result << std::endl;
  if (b) {
    BooleanFeature feature(result);
    this->featureMap->updateInsert(key, this->identifier, feature); 
    if ( result ) {
      this->parallelFeed(t);
    } else {
      BooleanFeature feature(0);
      this->featureMap->updateInsert(key, this->identifier, feature);  
    }
  }
  return true;
}


}

#endif
