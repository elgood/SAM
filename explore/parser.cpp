
#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/phoenix_core.hpp>
#include <boost/spirit/include/phoenix_operator.hpp>
#include <boost/spirit/include/phoenix_fusion.hpp>
#include <boost/spirit/include/phoenix_stl.hpp>
#include <boost/fusion/include/adapt_struct.hpp>
#include <boost/variant/recursive_variant.hpp>
#include <vector>
#include <string>

namespace qi = boost::spirit::qi;
namespace ascii = boost::spirit::ascii;
namespace spirit = boost::spirit;
namespace phoenix =  boost::phoenix;
namespace fusion = boost::fusion;

struct parameter_structure {
  std::vector<std::string> parameters;
};

BOOST_FUSION_ADAPT_STRUCT(
  parameter_structure,
  (std::vector<std::string>, parameters)
)

struct function_structure {
  std::string identifier;
  std::string function;
  parameter_structure parameters;
};

std::ostream& operator<< (std::ostream& os, const function_structure& f)
{
  os << "Identifier " << f.identifier << " Function: " << f.function;
  return os;
}

BOOST_FUSION_ADAPT_STRUCT(
  function_structure,
  (std::string, identifier)
  (std::string, function)
  (parameter_structure, parameters)
)

typedef
  boost::variant<
    std::string,
    function_structure,
    double>
OutputItem;

struct parse_structure {
  std::vector<OutputItem> tokens;
};

BOOST_FUSION_ADAPT_STRUCT(
  parse_structure,
  (std::vector<OutputItem>, tokens)
)

template <typename Iterator>
struct filter_grammar : qi::grammar<Iterator, parse_structure(), 
                                       ascii::space_type> 
{
  filter_grammar() : filter_grammar::base_type(expr)
  {
    using qi::lit;
    using qi::alnum;
    using qi::lexeme;
    using spirit::double_;
    using phoenix::at_c;
    using ascii::char_;
    using namespace qi::labels;
    using phoenix::push_back;

    expr = (function [push_back(at_c<0>(_val), _1)] |
           identifier [push_back(at_c<0>(_val), _1)] |
           double_ [push_back(at_c<0>(_val), _1)]) >>
           *(op [push_back(at_c<0>(_val), _1)] >>
              (function [push_back(at_c<0>(_val), _1)]  |
               identifier [push_back(at_c<0>(_val), _1)] |
               double_ [push_back(at_c<0>(_val), _1)]   
              )
            ); 
    //start = function [push_back(at_c<0>(_val), _1)];
    //start = +(identifier [push_back(at_c<0>(_val), _1)] |
    //          function );
    //start = +(identifier [push_back(at_c<0>(_val), _1)]);
    identifier = lexeme[+(alnum)  [_val += _1]];
    function = identifier [at_c<0>(_val) = _1] >> 
               qi::lit(".") >> 
               value_id [at_c<1>(_val) = _1] >> 
               (qi::lit("(") >> parameters [at_c<2>(_val) = _1] >> qi::lit(")")
                | qi::lit("(") >> qi::lit(")"));

    parameters = (identifier [push_back(at_c<0>(_val), _1)]) >> 
                    *(qi::lit(",") >>
                     (identifier [push_back(at_c<0>(_val), _1)]) ) |
                 (identifier [push_back(at_c<0>(_val), _1)]);

    value_id = qi::lit("value") [_val = "value"];
    op = qi::lit("+") [_val = "+"] |
         qi::lit("-") [_val = "-"] |
         qi::lit(">") [_val = ">"] |
         qi::lit("<") [_val = "<"] |
         qi::lit("<=") [_val = "<="] |
         qi::lit(">=") [_val = ">="] |
         qi::lit("*")  [_val = "*"] |
         qi::lit("/")  [_val = "/"] |
         qi::lit("^")  [_val = "^"];
               
  }

  qi::rule<Iterator, parse_structure(), ascii::space_type> expr;
  //qi::rule<Iterator, parse_structure(), ascii::space_type> start;
  qi::rule<Iterator, std::string(), ascii::space_type> identifier;
  qi::rule<Iterator, function_structure(), ascii::space_type> function;
  qi::rule<Iterator, std::string()> value_id;
  qi::rule<Iterator, parameter_structure(), ascii::space_type> parameters;
  qi::rule<Iterator, std::string()> op; 
};

int
main()
{
    std::cout << "////////////////////////////////////////////////////////\n\n";
    std::cout << "\t\t...\n\n";
    std::cout << "////////////////////////////////////////////////////////\n\n";

    std::cout << "Give me.\n";
    std::cout << "Type [q or Q] to quit\n\n";

    std::string str;
    while (getline(std::cin, str))
    {
        if (str.empty() || str[0] == 'q' || str[0] == 'Q')
            break;

        using boost::spirit::ascii::space;
        parse_structure result;
        filter_grammar<std::string::const_iterator> grammar;
        std::string::const_iterator iter = str.begin();
        std::string::const_iterator end = str.end();
        bool r = phrase_parse(iter, end, grammar, space, result);
        std::cout << "r " << r << " " << std::string(iter, end) << std::endl;
        if (r && iter == end)
        {
            std::cout << "-------------------------\n";
            std::cout << "Parsing succeeded\n";
            std::cout << str << " Parses OK: " << std::endl;
            for (auto s : result.tokens) {
              std::cout << s << std::endl;
            }
            std::cout << "\n-------------------------\n";
        }
        else
        {
            std::cout << "-------------------------\n";
            std::cout << "Parsing failed\n";
            std::cout << "-------------------------\n";
        }
    }

    std::cout << "Bye... :-) \n\n";
    return 0;
}
