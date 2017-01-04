#ifndef TOPK_H
#define TOPK_H

#include <vector>
#include <string>
#include <map>

#include "SlidingWindow.hpp"
#include "AbstractConsumer.h"
#include "BaseComputation.h"

using std::string;
using std::map;
using std::vector;
using std::shared_ptr;

namespace sam {

class TopK: public AbstractConsumer, public BaseComputation
{
private:
  size_t N; ///>Total number of elements
  size_t b; ///>Number of elements per window
  size_t k; ///>Top k elements managed

  map<string, shared_ptr<SlidingWindow<size_t>>> allWindows; 

public:
  TopK(size_t N, size_t b, size_t k,
       vector<size_t> keyFields,
       size_t valueField,
       size_t nodeId);
     

  bool consume(string s);

};

}

#endif
