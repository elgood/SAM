#include "TopK.h"
#include "Netflow.h"
#include <stdexcept>

namespace sam {

TopK::TopK(size_t N, size_t b, size_t k,
       vector<size_t> keyFields,
       size_t valueField,
       size_t nodeId) :
       BaseComputation(keyFields, valueField, nodeId)
{
  this->N = N;
  this->b = b;
  this->k = k;
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
  string key = generateKey(netflow);
  
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
