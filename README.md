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
* zmq

# Building

To build:  
cd SAM  
mkdir build  
cd build  
cmake ..  
make  

# Operators

Operators are sliding window algorithms that extract features from the stream of data.  Typically these operators are polylogarithmic in their spatial complexity and only use one pass through the data.  Currently supported operators are:

* Sum - Summation over the sliding window using expoential histograms.  This is implemented in ExponentialHistogramSum.hpp.
* Variance - Calculated again using an exponential histogram.  This is implmented in ExponentialHistogramVariance.hpp.
* TopK - Produces the most frequent keys and their associated frequencies.  Implemented using a Basic Window approach in TopK.hpp.

## Adding Operators

There are several polylog streaming algorithms that are candidates to implement, including:

* K-medians
* Quantiles
* Rarity
* Vector norms
* Similarity
* Count distinct items

Also, other approaches that are not necessarily polylog can also be added.  For example max, min, and autocorrelation are definite candidates.

To add a new operator, several interfaces need to be inherited, namely:

* AbstractConsumer
* BaseComputation
* FeatureProducer

The AbstractConsumer has one template argument, the TupleType, which is the type of tuple it is expected to consume.  There are three virtual methods in AbstractConsumer that must be implemented:

* consume(TupleType const& item) - The main method to implement.  This will have the logic for processing the item, updating associated data structures, and calculating the current value.  This method must also update the featuremap, a data structure provided by the BaseCompuation class. It must also notify any subscribers to this feature, which is provided by the FeatureProducer class.
* The destructor - Destructors have to be virtual in C++ or else bad things happen during object destruction.
* terminate - Usually does nothing.

