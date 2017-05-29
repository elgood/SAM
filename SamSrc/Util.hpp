#ifndef UTIL_HPP
#define UTIL_HPP

#include <boost/lexical_cast.hpp>

template <typename... T, std::size_t... I>
auto subtuple(std::tuple<T...> const& t, std::index_sequence<I...>) {
  return std::make_tuple(std::get<I>(t)...);
}

template <typename...  Ts>
std::string generateKey(std::tuple<Ts...> const& t)
{
  return "";
}


template <size_t keyField, size_t... keyFields, typename... Ts>
std::string generateKey(std::tuple<Ts...> const& t)
{
  std::string key = boost::lexical_cast<std::string>(std::get<keyField>(t));
  return key + generateKey<keyFields...>(t);
}




#endif
