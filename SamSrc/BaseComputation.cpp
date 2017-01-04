#include "BaseComputation.h"

namespace sam {

BaseComputation::BaseComputation(vector<size_t> keyFields,
                                 size_t valueField,
                                 size_t nodeId)
{
  this->keyFields = keyFields;
  this->valueField = valueField;
  this->nodeId = nodeId;

}

string BaseComputation::generateKey(Netflow const & netflow) const
{
  string key = "";
  for (auto i : keyFields) {
    key = key + netflow.getField(i);
  }
  return key;    
}

}
