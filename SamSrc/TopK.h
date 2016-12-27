#ifndef TOPK_H
#define TOPK_H

#include <vector>
#include <string>
#include <map>

#include "SlidingWindow.hpp"
#include "AbstractConsumer.h"

using std::string;
using std::map;
using std::vector;
using std::shared_ptr;

namespace sam {

class TopK: public AbstractConsumer 
{
private:
  size_t N; ///>Total number of elements
  size_t b; ///>Number of elements per window
  size_t k; ///>Top k elements managed

  size_t metricInterval = 100000;
  size_t feedCount = 0;

  vector<size_t> keyFields; ///> Index of fields that are the key
  size_t valueField; 
  string delimiter; ///> Used to tokenize each line
 
  /// This is mapping from ip's (as a string) to the correspdonding
  /// sliding window. 
  map<string, shared_ptr<SlidingWindow<size_t>>> allWindows; 

  size_t maxField; ///> The index of the largest field. 

  size_t nodeId; ///> Node id that is running this (for debugging)

public:
  TopK(size_t N, size_t b, size_t k,
       vector<size_t> keyFields,
       size_t valueField,
       string delimiter,
       size_t nodeId);
     

  bool consume(string s);

};

}

#endif
