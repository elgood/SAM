#ifndef SAM_ABSTRACT_SUBGRAPH_PRINTER_HPP
#define SAM_ABSTRACT_SUBGRAPH_PRINTER_HPP

#include <sam/SubgraphQueryResult.hpp>

namespace sam {

template <typename EdgeType, size_t source, size_t target, 
          size_t time, size_t duration>
class AbstractSubgraphPrinter
{
public:
  typedef SubgraphQueryResult<EdgeType, source, target, time,
                              duration> ResultType;

  /**
   * Prints the subgraph query result to some output medium
   * (e.g. stdout or disk).  Functionality to be defined by 
   * implementing classes.
   */
  virtual void print(ResultType const& result) = 0;

};

}

#endif
