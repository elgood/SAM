# Isaac Li Class Project CSCI 5320
#   The goal of this project is the extend functionality for the Streaming Analytics Machine (SAM) and 
#   the Streaming Analytics Language (SAL) by generating necessary header files based on inputs for new formats 
#   of data. Currently, this process is fully manual and very tedious. It is something that can be generated easily 
#   with simple inputs. 
#
#   This project can be run using:      python .\tupleStructureGenerator.py
#   
#   Follow the prompts and directions when creating a file. 
#   An example of inputs that create a correct file are as follows:
#
#   isaac
#   Test
#   std::string TestOne
#   std::string TestTwo
#   std::size_t TestThree
#   long TestFour
#   done
#   ,
#


def main():
    # Ask for a namespace name
    namespace_name = input("Input the name of your custom namesapce (should be lowercase): ")

    # Ask for the name of the tuple
    tuple_name = input("Input the name of the tuple (should begin with capital letter): ")


    # Ask for the name and type of features
    variable_declarations = []

    print("""Instructions: 
            1. Enter std:: before variable type wherever applicable. 
            2. Variable names must start with capital letter. 
            3. Only enter two words, one variable type, one variable name seperated by a space. 
          """)
    # Ask the user to enter variable declarations until they're done
    while True:
        variable_declaration = input("Enter a variable declaration in the format [type name] or [done] to finish: ")
        if variable_declaration.lower() == 'done':
            break
        else:
            variable_declarations.append(variable_declaration)



    # Ask for the delimiter used in the data file
    delimiter = input("Input the delimiter of your file (probably a comma) (do not add spaces, quotes, or parenthesis): ")


    #--------------------------
    # Construct the C++ code template 
    #------------------------------
    
    # Headers and namespaces
    cpp_code_template = f"""
    /**
    * This file provides the definition of {tuple_name}.
    */
    #ifndef SAM_{tuple_name.upper()}_HPP
    #define SAM_{tuple_name.upper()}_HPP   

    #include <string>
    #include <tuple>
    #include <iostream>
    #include <boost/tokenizer.hpp>
    #include <boost/foreach.hpp>
    #include <boost/lexical_cast.hpp>
    #include <boost/algorithm/string.hpp>
    #include <boost/fusion/algorithm/iteration/for_each.hpp>
    #include <zmq.hpp>

    #include <sam/Util.hpp>

    namespace sam {{

    namespace {namespace_name} {{
    
    """

    # Add variable declarations indices
    variable_declarations_with_indices = ""
    tuple_data_types = []
    for index, variable_declaration in enumerate(variable_declarations):
        variable_type, variable_name = variable_declaration.split()
        variable_declarations_with_indices += f"const unsigned int {variable_name} = {index};\n\t"
        tuple_data_types.append(variable_type)

    cpp_code_template += variable_declarations_with_indices

    # Add Exception
    cpp_code_template += f"""
    /**
    * We catch exceptions generated from this file and wrap them with a 
    * exception to make them easier to track.
    */
    class {tuple_name}Exception : public std::runtime_error {{
    public:
        {tuple_name}Exception(char const * message) : std::runtime_error(message) {{}} 
        {tuple_name}Exception(std::string  message) : std::runtime_error(message) {{}} 
    }}; \n \n
    """

    # Construct the tuple data type using the types from variable_declarations
    tuple_data_types_str = ', \n\t\t\t'.join(tuple_data_types)
    cpp_code_template += f"// Declare the tuple for future use\n\ttypedef std::tuple<{tuple_data_types_str}> {tuple_name};\n"

    # Create the function that creates the tuple from CSV
    cpp_code_template+= f"""
    /**
    * Converts a string that is in csv format into a tuple. 
    */
    inline
    {tuple_name} make{tuple_name}(std::string s) 
    {{
    \t// Temporary variables used to make the tuple
    """

    # Create the temporary variables
    temp_var_list=[]
    for variable_declaration in variable_declarations:
        variable_type, variable_name = variable_declaration.split()
        variable_name_lower = variable_name[0].lower() + variable_name[1:]  # Convert first letter to lowercase
        cpp_code_template += f"\t{variable_type} {variable_name_lower} ;\n\t"
        temp_var_list.append(variable_type+" "+variable_name_lower)

    # add string stream object and comments
    cpp_code_template+= f"""
        // Create StringStream object to read csv file
        std::stringstream ss(s);
        std::string item;

        // For each comma seperated value in the line, assign it to the appropriate variable
    """

    # for each variable, add another entry to retreive that value 
    for temp_var in temp_var_list:
        if temp_var.split()[0]=='std::string' or temp_var.split()[0]=='string':
            cpp_code_template += f"""  
        std::getline(ss, item, '{delimiter}');  
        try {{
            {temp_var.split()[1]} = item;
            }} catch (...) {{
                throw NetflowV5Exception(\"Troubles parsing {temp_var.split()[1]} with line: \" + s);
            }}\n\n"""
        else:
            cpp_code_template += f"""  
        std::getline(ss, item, '{delimiter}');  
        try {{
            {temp_var.split()[1]} = boost::lexical_cast<{temp_var.split()[0]}>(item);
            }} catch (...) {{
                throw NetflowV5Exception(\"Troubles parsing {temp_var.split()[1]} with line: \" + s);
            }}\n\n"""

    # create formatting for the finished tuple
    output_lines = [f"{var.split()[1]}" for var in temp_var_list]
    output = '\n\t\t'.join(output_lines)

    # write the end of function and a class constructor
    cpp_code_template+= f"""
        // finished tuple
        return std::make_tuple( 
        {output}
        );

    }} // end make{tuple_name}()

    class make{tuple_name}
    {{
        public:
            {tuple_name} operator()(std::string const& s)
            {{
                return make{tuple_name}(s); 
            }}
    }};
    """

    # End of file
    cpp_code_template+= f"""

    }} // end namespace {namespace_name}

    }} // end namespace sam
    #endif

    """ 

    # Write the C++ code to a file
    with open((tuple_name+".hpp"), "w") as file:
        file.write(cpp_code_template)

    print("SAM .hpp file generated successfully!")

main()