#ifndef BASE_COMPUTATION
#define BASE_COMPUTATION

#include <vector>
#include <string>

#include "Netflow.h"
#include "ImuxData.hpp"

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

  /// This is a reference to the map that stores the data for the
  /// imuxed data stream.
  ImuxData& imuxData;

  /// The variable name assigned to this operator.  This is specified
  /// in the query.
  string identifier; 

protected:
  string generateKey(Netflow const & n) const;


public:
  BaseComputation(vector<size_t> keyFields,
                  size_t valueFields,
                  size_t nodeId,
                  ImuxData& imuxData,
                  string identifier);
  virtual ~BaseComputation() {}

  

};


}


#endif
