# The Streaming Analytics Language (SAL) and the Streaming Analytics Machine
(SAM). 

# Contents

* ExecutableSrc - This contains the source for executables that utilize
* SAL - The SAL parser that converts SAL code into c++ code (SAM).
SAM.
* SamSrc - The source code for the SAM library.
* TestSrc - Unit tests.
* explore - Directory with code that might be useful but not part of SAM.
* proto - The protobuf description files.
* scripts - Largely python code that was used to perform analysis.

# Prerequisites

* Boost
* protobuf
* cmake

# Building

To build:  
cd SAM  
mkdir build  
cd build  
cmake ..  
make  
