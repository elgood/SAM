/*
 * ReadFile2.h
 *
 *  Created on: Nov 7, 2017
 *      Author: elgood
 */

#ifndef READFILE_HPP
#define READFILE_HPP

#include <string>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <fstream>
#include <fcntl.h>

#include <sam/BaseProducer.hpp>
#include <sam/VastNetflow.hpp>
#include <sam/AbstractDataSource.hpp>

#define BUFFER_SIZE 256

namespace sam {

class ReadFile2 : public BaseProducer<Netflow>, public AbstractDataSource
{
private:
	int readCount;
	std::string previous;
  int metricInterval = 100000;
  int fd;
  std::string filename;


public:
	ReadFile2(std::string filename) :
  BaseProducer(1) 
  {
    this->filename = filename;  
    readCount = 0;
  }
	virtual ~ReadFile2() {};

	bool connect();
  void receive();
};


bool ReadFile2::connect()
{
  fd = open(filename.c_str(), O_RDONLY);

  posix_fadvise(fd, 0, 0, 1);

	return true;
}

void ReadFile2::receive()
{
  int i = 0;
  std::string line;
  while(std::getline(myfile, line)) {
    Netflow netflow = makeNetflow(i, line);
    for (auto consumer : consumers) {
      consumer->consume(netflow);
    }
    i++;
    if (i % metricInterval == 0) {
      std::cout << "ReadFile2 received " << i << std::endl;
    }
  }
}


}

#endif /* READFILE_HPP */
