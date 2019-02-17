#ifndef SAM_NULL_HPP
#define SAM_NULL_HPP

#include <string>
#include <limits>

namespace sam {

/**
 * Should work for most numeric types.  We use the maximum value as the
 * null value.  There should hopefully not be a conflict with using this 
 * value as the represented null value.
 */
template <typename T>
T nullValue() { return std::numeric_limits<T>::max(); };

/**
 * The null value for a string is the empty string.
 */
template<>
std::string nullValue<std::string>() { return ""; }

/**
 * Returns true if the value  
 */
template <typename T>
bool isNull(T const& t) {
  return t == nullValue<T>();
}


}
#endif
