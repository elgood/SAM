/*
 * ReadSocket.h
 *
 *  Created on: Nov 12, 2016
 *      Author: elgood
 */

#ifndef READSOCKET_H_
#define READSOCKET_H_

#include <string>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>

#include "BaseProducer.hpp"
#include "Netflow.h"

#define BUFFER_SIZE 256

using std::string;

namespace sam {

class ReadSocket : public BaseProducer<Netflow> {
private:
	int port;
	string ip;
	int sockfd;
	char buffer[BUFFER_SIZE];
	char buffer2[2048];
	string bufferStr;
	int readCount;
	string previous;
  int metricInterval = 100000;

public:
	ReadSocket(string ip, int port);
	virtual ~ReadSocket();

	bool connect();
	string readline();
  void receive();
	/*string readline2();
	string readline3();
	string readline4();
	string readline5();*/
};

}

#endif /* READSOCKET_H_ */
