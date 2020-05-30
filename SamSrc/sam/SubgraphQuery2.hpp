#ifndef SAM_SUBGRAOH_QUERY2_HPP
#define SAM_SUBGRAOH_QUERY2_HPP

#include <sam/Expresssion.hpp>

namespace sam
{

/**
 * The first type of subgraph query defined time relative to a specified
 * zero time.  This subgraph query does not define a zero time, but 
 * temporal constraints are defined relative to edges start or end time.
 * For example, the old method went something like this:
 *
 * starttime(e1) = 0
 * endtime(e1) < 1
 * 
 * The above example with the zero-based time indexing says that the e1
 * must have a duraction less than 1 second.  The alternate way to
 * express that is below:
 *
 * endtime(e1) - starttime(e1) < 1
 */
template <typename TupleType>
class SubgraphQuery2
{
public:


}

}

#endif
