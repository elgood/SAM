#ifndef SAM_TUPLIZER_HPP
#define SAM_TUPLIZER_HPP

#include <sam/tuples/Edge.hpp>

namespace sam {

template <typename EdgeType, typename Function>
class TuplizerFunction
{
public:
  typedef typename EdgeType::LocalIdType IdType;
  typedef typename EdgeType::LocalLabelType LabelType;
  typedef typename EdgeType::LocalTupleType TupleType;

private:
  Function function;

public:
  
  EdgeType operator()(size_t id, std::string const& s) {
    
    // The result will have the label and the remainder of the string
    // that was not the label.
    LabelResult<LabelType> result = extractLabel<LabelType>(s);
    TupleType tuple = function(result.remainder);
    
    EdgeType edge(id, result.label, tuple);
    
    return edge;
  }
};

} // End namespace sam

#endif
