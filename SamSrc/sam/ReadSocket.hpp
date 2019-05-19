/*
 * ReadSocket.h
 *
 *  Created on: Nov 12, 2016
 *      Author: elgood
 */

#ifndef READSOCKET_HPP
#define READSOCKET_HPP

#include <string>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>

#include <sam/BaseProducer.hpp>
#include <sam/VastNetflow.hpp>
#include <sam/AbstractDataSource.hpp>

#define READ_SOCKET_BUFFER_SIZE 4096 

namespace sam {

class ReadSocket : public BaseProducer<std::string>, public AbstractDataSource
{
private:
	int port;
  int start = 0;
  int currentSizeBuffer = 0;
	std::string ip;
	int sockfd;
	char buffer[READ_SOCKET_BUFFER_SIZE];
	char buffer2[2048];
	std::string bufferStr;
	int readCount;
	std::string previous = "";
  int metricInterval = 100000;

public:
	ReadSocket(std::string ip, int port);
	virtual ~ReadSocket();

	bool connect();
	std::string readline();
  std::string readline2();
  void receive();
	/*string readline3();
	string readline4();
	string readline5();*/
};

ReadSocket::ReadSocket(std::string ip, int port) :
  BaseProducer(1)
{
	this->ip = ip;
	this->port = port;
	sockfd = -1;
	readCount = 0;
}

ReadSocket::~ReadSocket() {
}

bool ReadSocket::connect()
{

	struct sockaddr_in serv_addr;
	struct hostent *server;

	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd < 0) {
		std::cerr << "Error opening socket" << std::endl;
		return false;
	}
	server = gethostbyname(ip.c_str());

	if (server == NULL) {
		std::cerr << "No such host" << std::endl;
		return false;
	}

	bzero((char *) &serv_addr, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	bcopy((char *)server->h_addr,
		 (char *)&serv_addr.sin_addr.s_addr,
		 server->h_length);
	serv_addr.sin_port = htons(port);

	if (::connect(sockfd,(struct sockaddr *) &serv_addr,sizeof(serv_addr)) < 0) {
		std::cerr << "ERROR connecting" << std::endl;
		return false;
	}

	return true;
}
/*
string ReadSocket::readline2()
{
	readCount++;
	//std::cout << "read count " << readCount << std::endl;

	string line;

	string::iterator pos;
	while ((pos = std::find(bufferStr.begin(), bufferStr.end(), '\n')) == bufferStr.end()) {
		int numRead = read(sockfd, buffer, READ_SOCKET_BUFFER_SIZE);
		if (numRead <= 0) {
			line = "";
			return line;
		}
		buffer[numRead] = 0;
		bufferStr += buffer;
	}

	line = string(bufferStr.begin(), pos);
	bufferStr = string(pos + 1, bufferStr.end());
	//std::cout << " line " << line << std::endl;
	return line;

}

string ReadSocket::readline4()
{
	ssize_t numRead;
	size_t totRead;
	char *buf;
	char ch;

	if (buffer == NULL) {
		return "";
	}

	buf = buffer;

	totRead = 0;
	for (;;) {
		numRead = read(sockfd, &ch, 1);

		if (numRead == -1) {
			return "";

		} else if (numRead == 0) {
			if (totRead == 0)
				return "";
			else
				break;

		} else {
			if (totRead < READ_SOCKET_BUFFER_SIZE) {
				totRead++;
				*buf++ = ch;
			}

			if (ch == '\n')
				break;
		}
	}

	*buf = '\0';
	return string(buffer);
}

string ReadSocket::readline5()
{
	int n = 100;
	ssize_t numRead;
	size_t totRead;
	char *buf;
	char ch;

	if (buffer == NULL) {
		return "";
	}

	buf = buffer;

	totRead = 0;
	for (;;) {
		numRead = read(sockfd, buffer2, n);

		if (numRead == -1) {
			return "";

		} else if (numRead == 0) {
			if (totRead == 0)
				return "";
			else
				break;

		} else {
			if (totRead < READ_SOCKET_BUFFER_SIZE) {
				int i = 0;
				for (i = 0; i < numRead && buffer2[i] != '\n'; i++) {
					*buf++ = buffer2[i];
				}
				totRead += i;
				if (buffer2[i] == '\n') {
					buffer2[numRead] = '\0';
					previous = string(&buffer2[i+1]);
					break;
				}
			}

		}
	}

	*buf = '\0';
	return string(buffer);
}


*/
std::string ReadSocket::readline()
{
	readCount++;
	int numRead;

	if (previous.size() > 0) {
		int pos = previous.find("\n");
		if (pos > 0) {
			std::string rString = previous.substr(0, pos);
			if (rString[rString.size()-1] == '\r') {
				rString = rString.substr(0, rString.size()-1);
			}
			previous = previous.substr(pos + 1, previous.size() - (pos+1));
			return rString;
		}
	}

	//bzero(buffer,READ_SOCKET_BUFFER_SIZE);
	numRead = recv(sockfd,buffer,READ_SOCKET_BUFFER_SIZE - 1, 0);
	if (numRead > 0) {

		//int error = 0;
		//socklen_t len = sizeof (error);
		//int retval = getsockopt (sockfd, SOL_SOCKET, SO_ERROR, &error, &len);

		while (true) {
			buffer[numRead] = '\0';
			std::string temp(buffer);
			previous = previous + temp;
			int pos = previous.find("\n");
			if (pos > 0) {
				std::string rString = previous.substr(0, pos);
				if (rString[rString.size()-1] == '\r') {
					rString = rString.substr(0, rString.size()-1);
				}
				previous = previous.substr(pos + 1, previous.size() - (pos + 1));
				return rString;
			}
			//bzero(buffer,READ_SOCKET_BUFFER_SIZE);
			numRead = recv(sockfd,buffer,READ_SOCKET_BUFFER_SIZE - 1, 0);
			if (numRead <= 0) {
				return "";
			}
		}
	}
	return "";
}

std::string ReadSocket::readline2()
{
	readCount++;

  while (true) {
    if (start == 0) {
      int numRead = recv(sockfd,buffer,READ_SOCKET_BUFFER_SIZE - 1, 0);
      if (numRead <= 0) {
        return "";
      }
      currentSizeBuffer = numRead;
    }

    char* search = &buffer[start];
    char* newline = (char*) memchr(search, '\n', (currentSizeBuffer)-start);
    if (newline != NULL) {
      newline[0] = '\0';
      std::string rstring(&buffer[start]);
      rstring = previous + rstring;
      previous = "";
      start += newline-search + 1;
      //std::cout << "start " << start << std::endl;
      start = start % (currentSizeBuffer);   
      //std::cout << "start " << start << std::endl;
      return rstring;
    } else {
      buffer[currentSizeBuffer] = '\0';
      previous = std::string(&buffer[start]);
      //std::cout << "Previous " << previous << std::endl;
      start = 0;
    }
  }

}





void ReadSocket::receive()
{
  int i = 0;
  while(true) {
    std::string s = readline();
    //std::cout << "s in receive " << s << std::endl;
    if (s == "") {
      std::cout << "total in ReadSocket receive " << i << std::endl;
      return;
    }
    i++;
    //if (i % metricInterval == 0) {
    //  std::cout << "ReadSocket received " << i << std::endl;
    //}
    // This will generate a netflow with i as the sam generate id.
    // This label is thrown away later after the zeromq phase.
    //Netflow netflow = makeNetflow(i, s);
    for (auto consumer : consumers) {
      //consumer->consume(netflow);
      consumer->consume(s);
    }
  }
}


}

#endif /* READSOCKET_HPP */
