The Streaming Analytics Language (SAL) and the Streaming Analytics Machine
(SAM). 

Contents

* ExecutableSrc - This contains the source for executables that utilize
SAM.
* SamSrc - The source code for the SAM library.
* TestSrc - Unit tests.
* explore - Directory with code that might be useful but not part of SAM.
* proto - The protobuf description files.
* scripts - Largely python code that was used to perform analysis.

To build:  
cd SAM  
mkdir build  
cd build  
cmake ..  
make  
