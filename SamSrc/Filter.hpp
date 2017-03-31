#ifndef FILTER_HPP
#define FILTER_HPP

#include <string>
#include "FilterExpression.hpp"

using std::string;

namespace sam {

class Filter: public AbstractConsumer, BaseComputation 
{
private:
  FilterExpression const& expression;
public:
  Filter(FilterExpression const& _expression,
         vector<size_t> keyFields,
         size_t nodeId,
         ImuxData& imuxData,
         string identifier) :
         BaseComputation(keyFields, 0, nodeId, imuxData, identifier), 
         expression(_expression)
  {}

  bool consume(string s);

};

bool Filter::consume(string s) 
{
  Netflow netflow(s);
  string key = generateKey(netflow);
  shared_ptr<ImuxDataItem> item = imuxData.getDataItem(key);
  double result = expression.evaluate(*item); 

  if (!imuxData.existsFeature(key, identifier)) {
    std::shared_ptr<BooleanFeature> feature(new BooleanFeature(result));
    imuxData.addFeature(key, identifier, feature);
  } else {
    BooleanFeature feature(result);
    imuxData.updateFeature(key, identifier, feature); 
  }
  return true;
}


}

#endif
