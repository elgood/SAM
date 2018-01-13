/*
 * TestNCSpeed.cpp
 *
 *  Created on: Nov 12, 2016
 *      Author: elgood
 */

#include <iostream>
#include <string>
#include <boost/program_options.hpp>
#include <time.h>
#include <chrono>
#include <stdlib.h>

#include "ReadSocket.hpp"


namespace po = boost::program_options;
using std::string;
using namespace std::chrono;

int main(int argc, char* argv[])
{
	string ip;
	int port;
	int n;
	time_t timestamp_sec1, timestamp_sec2;

	po::options_description desc("Allowed options");
	desc.add_options()
		("help","help message")
		("ip", po::value<string>(&ip)->default_value("localhost"), "The ip to receive data")
		("port", po::value<int>(&port)->default_value(9999), "The port to receive data")
	;

	po::variables_map vm;
	po::store(po::parse_command_line(argc, argv, desc), vm);
	po::notify(vm);

	if (vm.count("help")) {
		std::cout << desc << std::endl;
		return 1;
	}

	sam::ReadSocket socket(ip, port);
	if (!socket.connect()) {
		std::cout << "Couldn't connect to " << ip << ":" << port << std::endl;
		return 1;
	}

  milliseconds ms1 = duration_cast<milliseconds>(
    system_clock::now().time_since_epoch()
  );
	string line = "";
	int count = 0;
  socket.receive();
  milliseconds ms2 = duration_cast<milliseconds>(
    system_clock::now().time_since_epoch()
  );
  std::cout << "Seconds " <<
    static_cast<double>(ms2.count() - ms1.count()) / 1000 << std::endl;


	return 0;
}



