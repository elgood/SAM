
#include <boost/lexical_cast.hpp>
#include <iostream>
#include <tuple>

#define TIME_SECONDS_FIELD 0
#define SOURCE_IP_FIELD 1
#define SOURCE_PORT 2


template <int I = 0, typename... Tp>
inline typename std::enable_if<I == sizeof...(Tp), std::string>::type
toString(std::tuple<Tp...> const&)
{
  return "";
}

template<int I = 0, typename... Tp>
inline typename std::enable_if<I < sizeof...(Tp), std::string>::type
toString(std::tuple<Tp...> const& t)
{
  std::string result = boost::lexical_cast<std::string>(std::get<I>(t));
  result = result + "," + 
  std::string end = toString<I + 1, Tp...>(t);
  if 
  return result;
}

int main(int argc, char** argv)
{
  auto t = std::make_tuple(16.7, "15.6.3.8", 99);

  std::cout << std::get<0>(t) << std::endl;

  std::get<0>(t) = 18.7;

  std::cout << std::get<0>(t) << std::endl;

  std::string s = toString(t);
  std::cout << s << std::endl;
}
