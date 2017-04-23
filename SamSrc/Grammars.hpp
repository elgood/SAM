#ifndef GRAMMARS_HPP
#define GRAMMARS_HPP

#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/phoenix_core.hpp>
#include <boost/spirit/include/phoenix_operator.hpp>
#include <boost/spirit/include/phoenix_fusion.hpp>
#include <boost/spirit/include/phoenix_stl.hpp>

#include "Tokens.hpp"

namespace sam {

// This is the grammar for parsing filter expressions.
template <typename Iterator>
struct FilterGrammar : boost::spirit::qi::grammar<Iterator, ParseStructure(),
                                 boost::spirit::ascii::space_type>
{
  FilterGrammar() : FilterGrammar::base_type(expr)
  {
    using boost::spirit::qi::lit;
    using boost::spirit::qi::alnum;
    using boost::spirit::qi::alpha;
    using boost::spirit::qi::lexeme;
    using boost::spirit::double_;
    using boost::phoenix::at_c;
    using boost::spirit::ascii::char_;
    using namespace boost::spirit::qi::labels;
    using boost::phoenix::push_back;
    using boost::spirit::qi::labels::_1;

    expr = (function [push_back(at_c<0>(_val), _1)] |
           identifier [push_back(at_c<0>(_val), _1)] |
           double_ [push_back(at_c<0>(_val), _1)]) >>
           *(op [push_back(at_c<0>(_val), _1)] >>
              (function [push_back(at_c<0>(_val), _1)]  |
               identifier [push_back(at_c<0>(_val), _1)] |
               double_ [push_back(at_c<0>(_val), _1)]
              )
            );
    identifier = lexeme[alpha [_val += _1] >> +(alnum)  [_val += _1]];
    function = identifier [at_c<0>(_val) = _1] >>
               lit(".") >>
               value_id [at_c<1>(_val) = _1] >>
               (lit("(") >> parameters [at_c<2>(_val) = _1] >> lit(")")
                | lit("(") >> lit(")"));

    parameters = (double_ [push_back(at_c<0>(_val), _1)]) >>
                    *(lit(",") >>
                     (double_ [push_back(at_c<0>(_val), _1)]));
                      

    value_id = lit("value") [_val = "value"];
    op = lit("+") [_val = "+"] |
         lit("-") [_val = "-"] |
         lit(">") [_val = ">"] |
         lit("<") [_val = "<"] |
         lit("<=") [_val = "<="] |
         lit(">=") [_val = ">="] |
         lit("*")  [_val = "*"] |
         lit("/")  [_val = "/"] |
         lit("^")  [_val = "^"];

  }

  boost::spirit::qi::rule<Iterator, ParseStructure(), 
                          boost::spirit::ascii::space_type> expr;
  boost::spirit::qi::rule<Iterator, std::string(), 
                          boost::spirit::ascii::space_type> identifier;
  boost::spirit::qi::rule<Iterator, FunctionStructure(), 
                          boost::spirit::ascii::space_type> function;
  boost::spirit::qi::rule<Iterator, std::string()> value_id;
  boost::spirit::qi::rule<Iterator, ParameterStructure(), 
                          boost::spirit::ascii::space_type> parameters;
  boost::spirit::qi::rule<Iterator, std::string()> op;
};

template <typename Iterator>
struct TransformGrammar : boost::spirit::qi::grammar<Iterator, 
                            ParseStructure(), 
                            boost::spirit::ascii::space_type>
{
  TransformGrammar() : TransformGrammar::base_type(expr)
  {
    using boost::spirit::qi::lit;
    using boost::spirit::qi::alnum;
    using boost::spirit::qi::alpha;
    using boost::spirit::qi::lexeme;
    using boost::spirit::double_;
    using boost::spirit::int_;
    using boost::phoenix::at_c;
    using boost::spirit::ascii::char_;
    using namespace boost::spirit::qi::labels;
    using boost::phoenix::push_back;
    using boost::spirit::qi::labels::_1;

    expr = (prev [push_back(at_c<0>(_val), _1)] |
            identifier [push_back(at_c<0>(_val), _1)] |
            double_ [push_back(at_c<0>(_val), _1)]) >>
            *(op [push_back(at_c<0>(_val), _1)] >>
              (prev [push_back(at_c<0>(_val), _1)] |
              identifier [push_back(at_c<0>(_val), _1)] |
              double_ [push_back(at_c<0>(_val), _1)])
            );
    identifier = lexeme[alpha [_val += _1] >> +(alnum)  [_val += _1]]; 
    prev = lit("prev") >> lit("(") >> int_ [at_c<0>(_val), _1] >>
           lit(")") >> lit(".") >> 
           identifier [at_c<1>(_val), _1];
           
           
    op = lit("+") [_val = "+"] |
         lit("-") [_val = "-"] |
         lit(">") [_val = ">"] |
         lit("<") [_val = "<"] |
         lit("<=") [_val = "<="] |
         lit(">=") [_val = ">="] |
         lit("*")  [_val = "*"] |
         lit("/")  [_val = "/"] |
         lit("^")  [_val = "^"];
  }

  boost::spirit::qi::rule<Iterator, ParseStructure(),
                          boost::spirit::ascii::space_type> expr;
  boost::spirit::qi::rule<Iterator, std::string(),
                          boost::spirit::ascii::space_type> identifier;
  boost::spirit::qi::rule<Iterator, PreviousStructure(),
                          boost::spirit::ascii::space_type> prev;
  boost::spirit::qi::rule<Iterator, std::string()> op;

};

}

#endif
