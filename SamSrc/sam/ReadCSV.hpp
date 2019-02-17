#ifndef SAM_READCSV_HPP
#define SAM_READCSV_HPP

#include <sam/BaseProducer.hpp>
#include <sam/Netflow.hpp>
#include <sam/AbstractDataSource.hpp>
#include <fstream>

namespace sam {

class ReadCSV : public BaseProducer<Netflow>, public AbstractDataSource
{
  std::string filename;
  std::ifstream file;
public:
  /**
   * \param filename The location of a CSV file.
   */
  ReadCSV(std::string _filename) : BaseProducer(1) {
    filename = _filename;
  }

  ~ReadCSV() {
    file.close();
  }

  bool connect() {
    file = std::ifstream(filename); 
    return true;
  }
  
  void receive()
  {
    int i = 0;
    std::string line;
    while(std::getline(file, line)) {
      //std::cout << "ReadCSV::receive got line " << line << std::endl;
      // We will use the order they come in as the SamGeneratedId.
      // This assumes that there is a label in each line.
      Netflow netflow = makeNetflow(i, line);
      //std::cout << "blah" << std::endl;
      for (auto consumer: consumers) {
        consumer->consume(netflow);
      }
      //i++;
      //std::cout << "ReadCSV i " << i << std::endl;
    }
    //consumer->consume(EMPTY_NETFLOW);
  }

};

}
#endif
