#ifndef SAM_EDGE_HPP
#define SAM_EDGE_HPP

#include <boost/lexical_cast.hpp>
#include <sam/Util.hpp>

namespace sam {

/**
 * Execption class for LabelException errors.
 */
class LabelException : public std::runtime_error {
public:
  LabelException(char const * message) : std::runtime_error(message) {}
  LabelException(std::string message) : std::runtime_error(message) {}
};

template <typename IdType, typename LabelType, typename TupleType>
class Edge
{
public:
  typedef IdType LocalIdType;
  typedef LabelType LocalLabelType;
  typedef TupleType LocalTupleType;

  IdType id;
  LabelType label;
  TupleType tuple;

  Edge(IdType id, LabelType label, TupleType tuple)
  {
    this->id = id;
    this->label = label;
    this->tuple = tuple;
  }

  Edge() {}

  /**
   * Combines the label and the tuple part together into a string.
   */
  std::string toString() const
  {
    std::string labelPart = tupleToString(this->label);
    std::string tuplePart = tupleToString(this->tuple);
    if (labelPart != "")
      return boost::lexical_cast<std::string>(id) + 
             "," + labelPart + "," + tuplePart;
    else
      return boost::lexical_cast<std::string>(id) + "," + tuplePart;
  }

  /**
   * Combines the label and the tuple part together into a string.
   */
  std::string toStringNoId() const
  {
    std::string labelPart = tupleToString(this->label);
    std::string tuplePart = tupleToString(this->tuple);
    if (labelPart != "")
      return labelPart + "," + tuplePart;
    else
      return tuplePart;
  }


};

/**
 * A class to extract labels.
 * \tparam LabelType The label type.  Usual an std::tuple.
 * \tparam N How many fields in the label.
 */
template <typename LabelType, int N>
class ExtractLabel
{
public:

  /**
   * Extracts the label from the beginning of the string.  The string
   * is modified so that the label is no longer present.  The label
   * is also modified to have the label fields.
   * \param s The comma separated string with the label at the front.
   * \param label A std::tuple that is populated by this method.
   */
  static void extract(std::string& s, LabelType& label)
  {
    size_t found = s.find(",");
    if (found == std::string::npos) {
      throw LabelException("Looking for delimiter but found none in string " +
         s);
    }
      
    std::string beg = s.substr(0, found);
    std::string end = s.substr(found + 1);
   
    typedef typename std::tuple_element<std::tuple_size<LabelType>::value - N, 
      LabelType>::type fieldType; 
    std::get<std::tuple_size<LabelType>::value - N>(label) = 
      boost::lexical_cast<fieldType>(beg);

    s = end;

    ExtractLabel<LabelType, N-1>::extract(s, label);

  }
};

/**
 * Base class for ExtractLabel.
 */
template <typename LabelType>
class ExtractLabel<LabelType, 0>
{
public:
  static void extract(std::string& s, LabelType& label)
  {

  }
};

/**
 * There are two things we need from extracting the label, the 
 * label itself and the string without the label at the front.  This
 * struct encapsulates both.
 */
template <typename LabelType>
struct
LabelResult
{
  LabelType label;
  std::string remainder;
};

/**
 * Convenience method for extracting the label.
 * \param s The comma-separated string with the label at the beginning.
 * \return Returns LabelResult, that has the label and the remainder of the 
 *   string without the label at the beginning.
 */
template <typename LabelType>
LabelResult<LabelType> extractLabel(std::string s)
{
  LabelType label;
  ExtractLabel<LabelType, std::tuple_size<LabelType>::value>::extract(s, label);

  LabelResult<LabelType> result;
  result.label = label;
  result.remainder = s;

  return result;
}

typedef std::tuple<bool> SingleBoolLabel; 
typedef std::tuple<int> SingleIntLabel; 
typedef std::tuple<> EmptyLabel;


}

#endif
