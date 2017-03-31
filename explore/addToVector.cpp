/*=============================================================================
    Copyright (c) 2002-2010 Joel de Guzman

    Distributed under the Boost Software License, Version 1.0. (See accompanying
    file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
=============================================================================*/
///////////////////////////////////////////////////////////////////////////////
//
//  A parser for summing a comma-separated list of numbers using phoenix.
//
//  [ JDG June 28, 2002 ]   spirit1
//  [ JDG March 24, 2007 ]  spirit2
//
///////////////////////////////////////////////////////////////////////////////

#include <boost/config/warning_disable.hpp>
//[tutorial_adder_includes
#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/phoenix_core.hpp>
#include <boost/spirit/include/phoenix_operator.hpp>
#include <boost/spirit/include/phoenix_stl.hpp>
#include <boost/lexical_cast.hpp>
#include <iostream>
#include <string>
#include <vector>
//]

using std::string;
using std::vector;

namespace client
{
    //[tutorial_adder_using
    namespace qi = boost::spirit::qi;
    namespace ascii = boost::spirit::ascii;
    namespace phoenix = boost::phoenix;

    using qi::double_;
    using qi::_1;
    using ascii::space;
    using phoenix::ref;
    using phoenix::push_back;
    //]

    ///////////////////////////////////////////////////////////////////////////
    //  Our adder
    ///////////////////////////////////////////////////////////////////////////

    //[tutorial_adder
    template <typename Iterator>
    bool adder(Iterator first, Iterator last, vector<string>& n)
    {
        bool r = qi::phrase_parse(first, last,

            //  Begin grammar
            (
                double_[push_back(phoenix::ref(n), 
                        boost::lexical_cast<string>(_1))] 
                  >> *(',' >> double_[push_back(phoenix::ref(n), 
                                      boost::lexical_cast<string>(_1))])
            )
            ,
            //  End grammar

            space,
            );

        if (first != last) // fail if we did not get a full match
            return false;
        return r;
    }
    //]
}

////////////////////////////////////////////////////////////////////////////
//  Main program
////////////////////////////////////////////////////////////////////////////
int
main()
{
    std::cout << "/////////////////////////////////////////////////////////\n\n";
    std::cout << "\t\tA parser for summing a list of numbers...\n\n";
    std::cout << "/////////////////////////////////////////////////////////\n\n";

    std::cout << "Give me a comma separated list of numbers.\n";
    std::cout << "The numbers are added using Phoenix.\n";
    std::cout << "Type [q or Q] to quit\n\n";

    std::string str;
    while (getline(std::cin, str))
    {
        if (str.empty() || str[0] == 'q' || str[0] == 'Q')
            break;

        vector<string> v;
        if (client::adder(str.begin(), str.end(), v))
        {
            std::cout << "-------------------------\n";
            std::cout << "Parsing succeeded\n";
            std::cout << str << " Parses OK: " << std::endl;

            for (auto i : v) std::cout << i << std::endl;
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
