#ifndef BASE_COMPUTATION
#define BASE_COMPUTATION

#include <vector>
#include <string>

#include "Netflow.h"

using std::vector;
using std::string;

namespace sam
{

class BaseComputation
{
protected:
  size_t metricInterval = 100000;

  vector<size_t> keyFields; ///> Index of fields that are in the key
  size_t valueField;  ///> The target field
  size_t nodeId; ///> Used for debugging/metrics per node

protected:
  string generateKey(Netflow const & n) const;


public:
  BaseComputation(vector<size_t> keyFields,
                  size_t valueFields,
                  size_t nodeId);
  virtual ~BaseComputation() {}

  

};


}


#endif
