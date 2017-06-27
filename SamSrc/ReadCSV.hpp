#ifndef SAM_READCSV_HPP
#define SAM_READCSV_HPP

#include "BaseProducer.hpp"
#include "Netflow.hpp"

namespace sam {

class ReadCSV : public BaseProducer<Netflow>
{
  std::ifstream file;
public:
  /**
   * \param filename The location of a CSV file.
   */
  ReadCSV(std::string filename) : BaseProducer<Netflow>(1) {
    file = std::ifstream(filename); 
  }

  ~ReadCSV() {
    file.close();
  }
  
  void receive()
  {
    int i = 0;
    std::string line;
    while(std::getline(file, line)) {
      Netflow netflow = makeNetflow(line);
      for (auto consumer: consumers) {
        consumer->consume(netflow);
      }
    }

  }

};

}
#endif
