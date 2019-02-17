#define BOOST_TEST_MAIN TestReadCSV
#include <boost/test/unit_test.hpp>
#include <string>
#include <fstream>
#include <sam/NetflowGenerators.hpp>
#include <sam/ReadCSV.hpp>

using namespace sam;

/**
 * Consumer that tests what it receives from consume is as expected
 * as provided by the array of strings passed to the constructor.
 */
class TestConsumer : public AbstractConsumer<Netflow>
{
private:
  std::string const * const array;
  int seen = 0;
public:
  TestConsumer(std::string const * const _array) : array(_array)
  {}
  
  bool consume(Netflow const& netflow) {
    BOOST_CHECK_EQUAL( tupleToString(netflow), array[seen]);
    seen += 1;  
    return true;  
  }

  // Don't need to do anything
  void terminate() {};
};

BOOST_AUTO_TEST_CASE( test_readcsv )
{
  
  // Creating a netflow generator to create some example netflows.
  int numPorts = 4;
  UniformDestPort generator("192.168.0.1", numPorts);

  // Opening a file to spit out the netflows to.
  std::ofstream myfile;
  std::string testfilename = "testreadcsv.csv";
  myfile.open(testfilename);

  // Generating the netflows
  int numNetflows = 4;
  std::string* stringArray = new std::string[numNetflows]; 
  for (int i = 0; i < numNetflows; i++) {
    std::string netflowString = generator.generate();
    stringArray[i] = netflowString + ",-1";
    myfile << netflowString << std::endl;
  }
  myfile.close();

  ReadCSV receiver(testfilename);

  auto consumer = std::make_shared<TestConsumer>(stringArray);
  receiver.registerConsumer(consumer);
  
  receiver.receive(); 

  delete[] stringArray;
  

}

