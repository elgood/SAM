#ifndef SAM_NULL_HPP
#define SAM_NULL_HPP

#include <string>

namespace sam {

template <typename T>
T nullValue();

template<>
std::string nullValue<std::string>() { return ""; }

template <typename T>
bool isNull(T t) {
  return t == nullValue<T>();
}


}
#endif
