/*******************************************************************/
// This files allows you to type in lines and it will return whether or
// not the line passes the filter grammar parser.
/*******************************************************************/

#include <FilterTokenizer.hpp>
#include <boost/spirit/include/qi_parse.hpp>
#include <string>

using boost::spirit::ascii::space;

int
main()
{
    std::cout << "////////////////////////////////////////////////////////\n\n";
    std::cout << "\t\t...\n\n";
    std::cout << "////////////////////////////////////////////////////////\n\n";

    std::cout << "Type in a line that is a filter expression.\n";
    std::cout << "Type [q or Q] to quit\n\n";

    std::string str;
    while (getline(std::cin, str))
    {
        if (str.empty() || str[0] == 'q' || str[0] == 'Q')
            break;

        sam::parse_structure result;
        sam::filter_grammar<std::string::const_iterator> 
          grammar;
        std::string::const_iterator iter = str.begin();
        std::string::const_iterator end = str.end();
        bool r = phrase_parse(iter, end, grammar, space, result);
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
