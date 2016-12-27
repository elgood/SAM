#include "TopK.h"
#include "Netflow.h"
#include <stdexcept>

namespace sam {

TopK::TopK(size_t N, size_t b, size_t k,
       vector<size_t> keyFields,
       size_t valueField,
       string delimiter,
       size_t nodeId)
{
  this->N = N;
  this->b = b;
  this->k = k;
  this->keyFields = keyFields;
  this->valueField = valueField;
  this->delimiter = delimiter;
  this->nodeId = nodeId;

  auto result = std::max_element(keyFields.begin(), keyFields.end());
  maxField = *result;
}

bool TopK::consume(string s) 
{
  feedCount++;
  if (feedCount % metricInterval == 0) {
    std::cout << "NodeId " << nodeId << " number of keys " 
              << allWindows.size() << std::endl;
  }
  Netflow netflow(s);

  // Creating a hopefully unique key from the key fields
  string key = "";
  for (auto i : keyFields) {
    key = key + netflow.getField(i);
  }

  
  if (allWindows.count(key) == 0) {
    auto sw = shared_ptr<SlidingWindow<size_t>>(
                new SlidingWindow<size_t>(N,b,k));
    std::pair<string, shared_ptr<SlidingWindow<size_t>>> p(key, sw);
    allWindows.insert(p);    
  }
  
  string sValue = netflow.getField(valueField);
  
  // TODO not very general
  size_t value = boost::lexical_cast<size_t>(sValue);
  
  auto sw = allWindows[key];
  sw->add(value);
  return true;
}

}
